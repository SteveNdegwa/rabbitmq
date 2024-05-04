from django.http import JsonResponse

from api.utils.common import get_request_data
from processors.producer import Producers


class GeneratePayload(object):
	@staticmethod
	def formulate_sms(request):
		d = dict()
		data = get_request_data(request)
		customer_id = data.get('customer_id')
		messages = data['message_list']
		failed_messages = list()
		for message in messages:
			d['message'] = message
			d['customer_id'] = customer_id
			resp = Producers().process_sms(**d)
			if resp.get('code') != "200.000":
				failed_messages.append(d)
				continue
		while len(failed_messages) > 0:
			msg = failed_messages.pop()
			Producers().process_sms(**msg)
		return JsonResponse({"code": "200.000", "message": "Success"})

	@staticmethod
	def formulate_document_data(request):
		d = dict()
		data = get_request_data(request)
		d["data"] = data
		resp = Producers().process_documents(**d)
		if resp.get('code') != "200.000":
			Producers().process_documents(**data)
		return JsonResponse({"code": "200.000", "message": "Success"})


from django.urls import path

urlpatterns = [
	path('sms/', GeneratePayload().formulate_sms),
	path('document/', GeneratePayload().formulate_document_data),
]
