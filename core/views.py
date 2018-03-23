import logging

from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from rest_framework.parsers import JSONParser
from rest_framework.renderers import JSONRenderer
from rest_framework import status
from core.models import Action, ActionType, ActionDetail, AddActionDetailRequest
from core.serializers import AddActionRequestSerializer, ActionSerializer

logger = logging.getLogger(__name__)


@api_view(['POST'])
@csrf_exempt
def add_action(request, action_type_name):
    logger.debug(f"Attempting to add an action for type name [{action_type_name}] ...")
    json = JSONParser().parse(request)
    logger.debug(f"Raw request [{json}]")
    add_action_request_serializer = AddActionRequestSerializer(data=json)
    if add_action_request_serializer.is_valid():
        logger.debug("Request is valid.")
        add_action_request = add_action_request_serializer.save()
        logger.debug(f"ActionRequest created [#{add_action_request.id}]")
        new_action = add_action_with_details(action_type_name, add_action_request)
        response = HttpResponse(status=status.HTTP_201_CREATED)
        response['Location'] = request.build_absolute_uri(f"/api/actions/{new_action.id}")
        return response
    else:
        logger.error(f"Request is invalid, with errors {add_action_request_serializer.errors}")
        return JsonResponse(add_action_request_serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
def get_action_by_id(request, action_id):
    logger.debug(f"Attempting to get Action by id {action_id}")
    try:
        action = Action.objects.get(id=action_id)
        data = ActionSerializer(action).data
        logger.debug(f"data {data}")
        json = JSONRenderer().render(data)
        logger.debug(f"json {repr(json)}")
        return JsonResponse(status=200, data=data, safe=False)
    except  Action.DoesNotExist:
        return HttpResponse(status=404)


def add_action_with_details(action_type_name, add_action_request):
    action_type = get_or_create_action_type(action_type_name)
    action = Action(action_type=action_type,
                    occurrence_date=add_action_request.occurrence_date,
                    actor_name=add_action_request.actor_name,
                    actor_email_address=add_action_request.actor_email_address,
                    actor_business_sso_uri=add_action_request.actor_business_sso_uri,
                    source=add_action_request.source
                    )
    action.save()
    logger.debug(f"Successfully created the new Action [#{action.id}]")
    add_action_details_requests = AddActionDetailRequest.objects.filter(add_action_request=add_action_request)
    for add_action_details_request in add_action_details_requests:
        action_detail = add_action_detail(action, add_action_details_request)
        logger.debug(f"Added ActionDetail [#{action_detail.id}]")

    return action


def get_or_create_action_type(action_type_name):
    normalised_action_type_name = action_type_name.strip().lower()
    logger.debug(f"Looking for an ActionType with name [{normalised_action_type_name}]")
    try:
        existing_action_type = ActionType.objects.get(name=normalised_action_type_name)
        logger.debug(f"... yes, it exists: [#{existing_action_type.id}]")
        return existing_action_type
    except ActionType.DoesNotExist:
        logger.debug("... no, it doesn't exist, so creating a new one ...")
        new_action_type = ActionType(name=normalised_action_type_name)
        new_action_type.save()
        logger.debug(f"... created: [#{new_action_type.id}]")
        return new_action_type


def add_action_detail(action, add_action_detail_request):
    action_detail = ActionDetail(action=action,
                                 key=add_action_detail_request.key,
                                 value=add_action_detail_request.value)
    action_detail.save()
    return action_detail
