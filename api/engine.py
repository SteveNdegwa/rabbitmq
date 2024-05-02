from django.http import JsonResponse

from api.utils.common import get_request_data
from queue.producer import Producers


class GeneratePayload(object):
	@staticmethod
	def formulate_sms(request):
		d = dict()
		data = get_request_data(request)
		customer_id = data.get('customer_id')
		messages = data.get('message_list')
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



