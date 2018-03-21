from django.urls import path

from . import views

urlpatterns = [
    path('actions/<str:action_type_name>/', views.add_action),
    path('actions/<int:action_id>', views.get_action_by_id),
]
