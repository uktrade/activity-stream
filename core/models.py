from django.db import models


class ActionType(models.Model):
    name = models.CharField(null=False, blank=False, unique=True, max_length=255)

    class Meta:
        db_table = 'action_type'


class Action(models.Model):
    action_type = models.ForeignKey(ActionType, null=False, on_delete=models.PROTECT)
    recording_date = models.DateTimeField(auto_now=True)
    occurrence_date = models.DateTimeField(null=True)
    actor_name = models.CharField(null=True, blank=False, max_length=255)
    actor_email_address = models.EmailField(null=True, blank=False, max_length=255)
    actor_business_sso_uri = models.URLField(null=True, blank=False, max_length=255)
    source = models.CharField(null=True, blank=False, max_length=255)

    class Meta:
        db_table = 'action'


class ActionDetail(models.Model):
    action = models.ForeignKey(Action, related_name='action_details', null=False, on_delete=models.PROTECT)
    key = models.CharField(null=False, blank=False, max_length=255)
    value = models.TextField(null=False, blank=False)

    class Meta:
        db_table = 'action_detail'


class AddActionRequest(models.Model):
    source = models.CharField(null=True, blank=False, max_length=255)
    occurrence_date = models.DateTimeField(null=True)
    actor_name = models.CharField(null=True, blank=False, max_length=255)
    actor_email_address = models.EmailField(null=True, blank=False, max_length=255)
    actor_business_sso_uri = models.URLField(null=True, blank=False, max_length=255)

    class Meta:
        db_table = 'add_action_request'


class AddActionDetailRequest(models.Model):
    add_action_request = models.ForeignKey(AddActionRequest, related_name='details', null=False, on_delete=models.PROTECT)
    key = models.CharField(null=False, blank=False, max_length=255)
    value = models.TextField(null=False, blank=False)

    class Meta:
        db_table = 'add_action_detail_request'
