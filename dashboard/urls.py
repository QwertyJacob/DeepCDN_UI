from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard_with_pivot, name='dashboard_with_pivot'),
    path('data', views.pivot_data, name='pivot_data'),
    path('msg_data', views.messages_data, name='msg_data'),

]