from rest_framework import serializers

from core.models import AddActionRequest, AddActionDetailRequest, Action, ActionType, ActionDetail


class ActionTypeSerializer(serializers.ModelSerializer):
    class Meta:
        model = ActionType
        fields = ('id', 'name')


class ActionDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = ActionDetail
        fields = ('key', 'value')


class ActionSerializer(serializers.ModelSerializer):
    action_type = ActionTypeSerializer(many=False, read_only=True)
    action_details = ActionDetailSerializer(many=True, read_only=True)

    class Meta:
        model = Action
        fields = (
            'action_type',
            'action_details',
            'actor_name',
            'actor_email_address',
            'actor_business_sso_uri',
            'id',
            'occurrence_date',
            'recording_date',
            'source')


class AddActionDetailRequestSerializer(serializers.ModelSerializer):
    class Meta:
        model = AddActionDetailRequest
        fields = ('id','key', 'value')


class AddActionRequestSerializer(serializers.ModelSerializer):
    details = AddActionDetailRequestSerializer(many=True, read_only=False)

    def create(self, validated_data):
        all_details_data = validated_data.pop('details')
        add_action_request = AddActionRequest.objects.create(**validated_data)
        for details_data_as_ordered_dict in all_details_data:
            details_data = dict(details_data_as_ordered_dict)
            AddActionDetailRequest.objects.create(add_action_request=add_action_request, **details_data)
        return add_action_request

    class Meta:
        model = AddActionRequest
        fields = ('source', 'occurrence_date', 'actor_name', 'actor_email_address', 'actor_business_sso_uri', 'details')
