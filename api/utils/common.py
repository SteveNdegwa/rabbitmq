# coding=utf-8
import json
from datetime import datetime, timedelta, date

from decimal import Decimal
from django.http import QueryDict


def get_request_data(request):
	try:
		data = None
		if request is not None:
			request_meta = getattr(request, 'META', {})
			request_method = getattr(request, 'method', None)
			if request_meta.get('CONTENT_TYPE', '') == 'application/json':
				data = json.loads(request.body)
			elif str(request_meta.get('CONTENT_TYPE', '')).startswith('multipart/form-data;'):  # Special handling for
				# Form Data?
				data = request.POST.copy()
				data = data.dict()
			elif request_method == 'GET':
				data = request.GET.copy()
				data = data.dict()
			elif request_method == 'POST':
				data = request.POST.copy()
				data = data.dict()
			if not data:
				request_body = getattr(request, 'body', None)
				if request_body:
					data = json.loads(request_body)
				else:
					data = QueryDict()
			return data
	except Exception as e:
		print('get_request_data Exception: %s', e)
	return QueryDict()