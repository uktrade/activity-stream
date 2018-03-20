from django.http import HttpResponse


def index(request):
    return HttpResponse("This will be the Activity Stream API.")