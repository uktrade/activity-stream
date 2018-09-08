import asyncio
from collections import (
    defaultdict,
    namedtuple,
)
import socket
import ssl
import re

import yarl

HttpSession = namedtuple(
    'HttpSession', ['close', 'request'],
)


def http_session():
    loop = asyncio.get_event_loop()
    max_send_size = 8192
    max_recv_size = 66560
    chunk_size_regex = re.compile(b'([0-9a-fA-F]+)\r\n')
    header_regex = re.compile(b'([^:]+): *([^\r]*)\r\n')
    status_regex = re.compile(b'HTTP/1.1 (\\d\\d\\d)')

    # Dict of host, port, scheme -> list of sockets
    pool = defaultdict(list)

    async def _get_socket(host, port, scheme):
        nonlocal pool
        pool_host = pool[(host, port, scheme)]
        return pool_host.pop(0) if pool_host else await _create_socket(host, port, scheme)

    async def _create_socket(host, port, scheme):
        sock_unencrypted = socket.socket(family=socket.AF_INET,
                                         type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP)
        sock_unencrypted.setblocking(False)
        await loop.sock_connect(sock_unencrypted, (host, port))

        if scheme == 'https':
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            sock = ssl_context.wrap_socket(
                sock_unencrypted, do_handshake_on_connect=False, server_hostname=host)
            while True:
                try:
                    sock.do_handshake()
                    break
                except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                    # We could have something more efficient, e.g. doing things
                    # with the asyncio readers, but it works
                    await asyncio.sleep(0)

        else:
            sock = sock_unencrypted

        return sock

    def _return_socket(sock, host, port, scheme):
        nonlocal pool
        pool[(host, port, scheme)].append(sock)

    async def _send_http_headers_body(sock, method, host, path, headers, body):
        headers_top = \
            method.encode('ascii') + b' ' + path + b' HTTP/1.1\r\n' + \
            b'Host: ' + host + b'\r\n' + \
            b'Content-Length: ' + str(len(body)).encode('ascii') + b'\r\n'
        headers_bottom = b''.join((
            key.encode('ascii') + b': ' + value.encode('ascii') + b'\r\n'
            for key, value in headers.items()
        ))
        header = headers_top + headers_bottom + b'\r\n'

        await _send(sock, header)
        await _send(sock, body)

    def _send(sock, buf):
        cursor = 0
        buf_memoryview = memoryview(buf)
        final_pos = len(buf)

        lock = asyncio.Lock()
        result = asyncio.Future()
        fileno = sock.fileno()

        def _on_write_available():
            loop.remove_writer(fileno)
            loop.create_task(_locked_write())

        async def _locked_write():
            async with lock:
                if result.done():
                    return
                try:
                    await _write()
                except BaseException as exception:
                    result.set_exception(exception)

        async def _write():
            nonlocal cursor

            max_end_pos = min(cursor + max_send_size, final_pos)
            try:
                to_send = buf_memoryview[cursor:max_end_pos]
                num_sent = sock.send(to_send) if to_send else 0
            except (ssl.SSLWantWriteError, BlockingIOError, InterruptedError):
                num_sent = 0

            cursor += num_sent

            if cursor == final_pos:
                result.set_result(None)
            else:
                loop.add_writer(fileno, _on_write_available)

        loop.add_writer(fileno, _on_write_available)

        return result

    def _recv_http_headers_body(sock):
        parser = None

        to_be_parsed_buf = bytearray(max_recv_size)
        to_be_parsed_end_pos = 0
        to_be_parsed_cursor = 0
        headers = {}
        header_buf = bytearray()
        chunk_bufs = []
        chunk_cursor = 0
        chunk_length = None

        chunk_header_start_pos = 0
        chunk_footer_start_pos = 0

        result = asyncio.Future()
        lock = asyncio.Lock()
        fileno = sock.fileno()

        def _on_read_available():
            loop.remove_reader(fileno)
            loop.create_task(_locked_read())

        async def _locked_read():
            async with lock:
                if result.done():
                    return
                try:
                    await _read()
                except BaseException as exception:
                    result.set_exception(exception)

        async def _read():
            nonlocal parser
            nonlocal to_be_parsed_buf
            nonlocal to_be_parsed_end_pos
            nonlocal to_be_parsed_cursor

            while True:
                try:
                    space_remaining = len(to_be_parsed_buf) - to_be_parsed_end_pos
                    num_bytes = sock.recv_into(
                        memoryview(to_be_parsed_buf)[to_be_parsed_end_pos:], space_remaining)
                except (ssl.SSLWantReadError, BlockingIOError, InterruptedError):
                    num_bytes = 0

                if not num_bytes:
                    break

                to_be_parsed_end_pos += num_bytes

                while to_be_parsed_cursor != to_be_parsed_end_pos:
                    parser()

                if parser == _parse_done:
                    _parse_done()
                    break

            if parser != _parse_done:
                loop.add_reader(fileno, _on_read_available)

        def _parse_header():
            nonlocal parser
            nonlocal to_be_parsed_cursor
            nonlocal header_buf
            nonlocal chunk_length
            nonlocal chunk_header_start_pos

            header_end_pos = to_be_parsed_buf.find(b'\r\n\r\n')
            has_header = header_end_pos != -1
            if not has_header:
                to_be_parsed_cursor = to_be_parsed_end_pos
                return

            header_length = header_end_pos + 4
            header_buf = to_be_parsed_buf[0:header_length]
            to_be_parsed_cursor = header_length
            chunk_header_start_pos = to_be_parsed_cursor

            headers_start_pos = to_be_parsed_buf.find(b'\r\n') + 2
            headers = {
                match[1].lower(): match[2]
                for match in
                header_regex.finditer(header_buf, headers_start_pos, header_end_pos + 2)
            }
            is_chunked = \
                b'transfer-encoding' in headers and \
                headers[b'transfer-encoding'] == b'chunked'
            chunk_length = \
                int(headers[b'content-length']) if b'content-length' in headers else \
                0
            chunk_bufs.append(bytearray(chunk_length))
            parser = _parse_chunked_header if is_chunked else _parse_non_chunked_body

        def _parse_non_chunked_body():
            nonlocal parser
            nonlocal chunk_cursor
            nonlocal to_be_parsed_buf
            nonlocal to_be_parsed_end_pos
            nonlocal to_be_parsed_cursor
            nonlocal chunk_cursor

            num_bytes = to_be_parsed_end_pos - to_be_parsed_cursor
            chunk_bufs[0][chunk_cursor:chunk_cursor + num_bytes] = \
                to_be_parsed_buf[to_be_parsed_cursor:to_be_parsed_end_pos]
            chunk_cursor += num_bytes
            to_be_parsed_end_pos = 0
            to_be_parsed_cursor = 0
            if chunk_cursor == chunk_length:
                parser = _parse_done

        def _parse_chunked_header():
            nonlocal parser
            nonlocal chunk_length
            nonlocal chunk_cursor
            nonlocal to_be_parsed_buf
            nonlocal to_be_parsed_cursor

            chunked_header_end_pos = to_be_parsed_buf.find(b'\r\n', chunk_header_start_pos)
            has_chunked_header = chunked_header_end_pos != -1
            if not has_chunked_header:
                to_be_parsed_cursor = to_be_parsed_end_pos
                return

            chunk_header_length = chunked_header_end_pos - chunk_header_start_pos + 2
            chunk_size_match = chunk_size_regex.match(
                to_be_parsed_buf, chunk_header_start_pos)
            chunk_length = int(chunk_size_match[1], 16)

            to_be_parsed_cursor = chunk_header_start_pos + chunk_header_length
            chunk_bufs.append(bytearray(chunk_length))
            chunk_cursor = 0
            parser = _parse_chunked_body

        def _parse_chunked_body():
            nonlocal parser
            nonlocal chunk_cursor
            nonlocal to_be_parsed_buf
            nonlocal to_be_parsed_cursor
            nonlocal chunk_footer_start_pos

            remaining_total_bytes = to_be_parsed_end_pos - to_be_parsed_cursor
            remaining_chunk_bytes = chunk_length - chunk_cursor
            num_bytes = min(remaining_total_bytes, remaining_chunk_bytes)
            chunk_bufs[-1][chunk_cursor:chunk_cursor+num_bytes] = \
                to_be_parsed_buf[to_be_parsed_cursor:to_be_parsed_cursor+num_bytes]
            chunk_cursor += num_bytes
            to_be_parsed_cursor += num_bytes

            if chunk_cursor != chunk_length:
                return

            parser = _parse_chunk_footer
            chunk_footer_start_pos = to_be_parsed_cursor

        def _parse_chunk_footer():
            nonlocal parser
            nonlocal to_be_parsed_buf
            nonlocal to_be_parsed_end_pos
            nonlocal to_be_parsed_cursor
            nonlocal chunk_header_start_pos

            chunk_footer_length = 2
            num_bytes = min(to_be_parsed_end_pos - chunk_footer_start_pos, chunk_footer_length)
            if num_bytes != chunk_footer_length:
                to_be_parsed_cursor = to_be_parsed_end_pos
                return

            bytes_remaining = to_be_parsed_end_pos - chunk_footer_start_pos - chunk_footer_length
            to_be_parsed_buf[:bytes_remaining] = \
                to_be_parsed_buf[chunk_footer_start_pos + chunk_footer_length:to_be_parsed_end_pos]
            to_be_parsed_end_pos = bytes_remaining
            chunk_header_start_pos = 0
            to_be_parsed_cursor = 0
            parser = _parse_chunked_header if chunk_bufs[-1] else _parse_done

        def _parse_done():
            status = int(status_regex.match(header_buf)[1])
            result.set_result((
                status,
                headers,
                b''.join(chunk_bufs),
            ))

        parser = _parse_header
        loop.add_reader(fileno, _on_read_available)

        return result

    async def request(method, url, headers, body):
        parsed_url = yarl.URL(url)
        path = parsed_url.raw_path_qs.encode('ascii')
        host = parsed_url.raw_host.encode('ascii')
        port = parsed_url.port
        scheme = parsed_url.scheme

        sock = None
        try:
            sock = await _get_socket(host, port, scheme)
            await _send_http_headers_body(sock, method, host, path, headers, body)
            status, response_headers, response_body = await _recv_http_headers_body(sock)
        except BaseException:
            if sock is not None:
                sock.close()
            raise

        should_close = \
            'connection' in response_headers and response_headers['connection'] == 'close'
        if should_close:
            sock.close()
        else:
            _return_socket(sock, host, port, scheme)

        return status, response_headers, response_body

    def close():
        nonlocal pool
        for _, socks in pool.items():
            for sock in socks:
                sock.close()

        pool = defaultdict(list)

    return HttpSession(close=close, request=request)
