from django.http import JsonResponse
from django.shortcuts import render
from dashboard.models import Order
from django.core import serializers
from kafka import KafkaConsumer
from json import loads
import random

def pivot_data(request):
    dataset = Order.objects.all()
    data = serializers.serialize('json', dataset)
    return JsonResponse(data, safe=False)


def dashboard_with_pivot(request):
    return render(request, 'dashboard_with_pivot.html', {})


def messages_data(request):
    consumer = KafkaConsumer(
        'channel_performance',
        bootstrap_servers=['kafka_kafkanet:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group' + str(random.randint(1, 100001)),
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    messages_list = []
    for message in consumer:
        if len(messages_list) < 10:
            messages_list.append(message.value)
        else:
            break

    return JsonResponse(messages_list, safe=False)
