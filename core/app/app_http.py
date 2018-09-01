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
    max_recv_size = 65536

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
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2,)
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

    def _recv_http_header_body(sock):
        to_be_processed_buf = bytearray()
        to_be_processed_cursor = 0
        header_buf = bytearray()
        body_buf = bytearray()
        body_cursor = 0
        section = 'header'
        header_length = None
        content_length = None

        result = asyncio.Future()
        lock = asyncio.Lock()

        def _on_read_available():
            loop.remove_reader(sock.fileno())
            loop.create_task(_wait_to_read())

        async def _wait_to_read():
            async with lock:
                if not result.done():
                    await _read()

        async def _read():
            nonlocal to_be_processed_buf
            nonlocal to_be_processed_cursor
            nonlocal section
            nonlocal header_buf
            nonlocal body_buf

            while True:
                try:
                    incoming_buf = await loop.sock_recv(sock, max_recv_size)
                except (ssl.SSLWantReadError, BlockingIOError):
                    incoming_buf = bytearray()

                if not incoming_buf:
                    break

                to_be_processed_buf.extend(incoming_buf)
                while to_be_processed_cursor != len(to_be_processed_buf):
                    _process()

                if section == 'done':
                    status = int(re.match(b'HTTP/1.1 (\\d\\d\\d)', header_buf)[1])
                    result.set_result((
                        status,
                        bytes(header_buf),
                        bytes(body_buf),
                    ))
                    break

                await asyncio.sleep(0)

            if section != 'done':
                loop.add_reader(sock.fileno(), _on_read_available)

        def _process():
            nonlocal to_be_processed_buf
            nonlocal to_be_processed_cursor

            nonlocal header_buf
            nonlocal body_buf
            nonlocal body_cursor
            nonlocal section
            nonlocal header_length
            nonlocal content_length

            if section == 'header':
                header_length = to_be_processed_buf.find(b'\r\n\r\n') + 4
                has_header = header_length != 3
                if not has_header:
                    to_be_processed_cursor = len(to_be_processed_buf)
                else:
                    header_buf = to_be_processed_buf[0:header_length]
                    to_be_processed_cursor = header_length
                    length_match = re.search(b'\r\nContent-Length: (\\d+)\r\n', header_buf)
                    content_length = int(length_match[1]) if length_match else 0
                    body_buf = bytearray(content_length)
                    section = 'non-chunked-body'

            elif section == 'non-chunked-body':
                num_bytes = len(to_be_processed_buf) - to_be_processed_cursor
                body_buf[body_cursor:body_cursor + num_bytes] = \
                    to_be_processed_buf[to_be_processed_cursor:]
                body_cursor += num_bytes
                to_be_processed_buf = bytearray()
                to_be_processed_cursor = 0
                if body_cursor == content_length:
                    section = 'done'

        loop.add_reader(sock.fileno(), _on_read_available)

        return result

    async def request(method, url, headers, body):
        parsed_url = yarl.URL(url)
        path = parsed_url.raw_path_qs.encode('ascii')
        host = parsed_url.raw_host.encode('ascii')
        port = parsed_url.port
        scheme = parsed_url.scheme

        headers_top = \
            method.encode('ascii') + b' ' + path + b' HTTP/1.1\r\n' + \
            b'Host: ' + host + b'\r\n' + \
            b'Content-Length: ' + str(len(body)).encode('ascii') + b'\r\n'
        headers_bottom = b''.join((
            key.encode('ascii') + b': ' + value.encode('ascii') + b'\r\n'
            for key, value in headers.items()
        ))
        header = headers_top + headers_bottom + b'\r\n'

        sock = None
        try:
            sock = await _get_socket(host, port, scheme)
            await loop.sock_sendall(sock, header)
            await loop.sock_sendall(sock, body)
            status, response_header, response_body = await _recv_http_header_body(sock)
        except BaseException:
            if sock is not None:
                sock.close()
            raise
        else:
            _return_socket(sock, host, port, scheme)

        return status, response_header, response_body

    def close():
        nonlocal pool
        for _, socks in pool.items():
            for sock in socks:
                sock.close()

        pool = {}

    return HttpSession(close=close, request=request)
