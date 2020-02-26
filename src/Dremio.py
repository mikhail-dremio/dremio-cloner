########
# Copyright (C) 2019-2020 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
########

import requests
import logging
import urllib
import json
import time


###
# Dremio API wrapper.
# TODO: replace with dremio-client
###
class Dremio:
	# API URLs
	_catalog_url = 'api/v3/catalog/'
	_catalog_url_by_path = 'api/v3/catalog/by-path/'
	_login_url = 'apiv2/login'
	_reflections_url = "api/v3/reflection/"
	_wlm_queue_url = "api/v3/wlm/queue/"
	_wlm_rule_url = "api/v3/wlm/rule"
	_wlm_vote_url = "api/v3/vote"
	_user_url = "api/v3/user/"
	_user_by_name_url = "api/v3/user/by-name/"
	_group_url = "api/v3/group/"
	_group_by_name_url = "api/v3/group/by-name/"
	_post_sql_url = "api/v3/sql"
	_get_job_url = "api/v3/job/"
	_graph_url_postfix = "graph"
	_endpoint = ""
	_authtoken = ""
	_headers = ""
	_verify_ssl = None
	# Dremio Config
	_api_timeout = None 			# Default 10 seconds
	_retry_timedout_source = None 	# Do not retry SOURCE that has timed out in previous API calls. Default False
	errors_encountered = 0
	# Misc
	_timed_out_sources = []

	def __init__(self, endpoint, username, password, api_timeout=10, retry_timedout_source=False, verify_ssl=True):
		if not verify_ssl:
			logging.warn("Unverified HTTPS requests will be made as per configuration.")
			requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
		self._endpoint = endpoint
		self._verify_ssl = verify_ssl
		self._api_timeout = api_timeout
		self._retry_timedout_source = retry_timedout_source
		headers = {"Content-Type": "application/json"}
		payload = '{"userName": "' + username + '","password": "' + password + '"}'
		response = requests.request("POST", self._endpoint + self._login_url, data=payload, headers=headers, timeout=self._api_timeout, verify=verify_ssl)
		if response.status_code != 200:
			logging.critical("Authentication Error " + str(response.status_code))
			raise RuntimeError("Authentication error.")
		self._authtoken = '_dremio' + response.json()['token']
		# print(self._authtoken)
		self._headers = {"Content-Type": "application/json", "Authorization": self._authtoken}

	def _build_url(self, url):
		return self._endpoint + url

	def list_catalog(self):
		return self._api_get_json(self._catalog_url, source="list_catalog")

	def get_catalog_entity_by_path(self, path, report_error=True):
		return self._api_get_json(self._catalog_url_by_path + path, source="get_catalog_entity_by_path", report_error=report_error)

	def get_catalog_entity_by_id(self, entity_id):
		if entity_id[:7] == 'dremio:':
			return self.get_catalog_entity_by_path(entity_id[8:])
		else:
			return self._api_get_json(self._catalog_url + entity_id, source="get_catalog_entity_by_id")

	def get_catalog_entity_graph_by_id(self, entity_id, report_error=True):
		return self._api_get_json(self._catalog_url + entity_id + '/' + self._graph_url_postfix, source="get_catalog_entity_graph", report_error=report_error)

	def get_user(self, user_id):
		return self._api_get_json(self._user_url + user_id, source="get_user")

	def get_user_by_name(self, username):
		return self._api_get_json(self._user_by_name_url + username, source="get_user_by_name")

	def get_group(self, group_id):
		return self._api_get_json(self._group_url + group_id, source="get_group")

	def get_group_by_name(self, groupname):
		return self._api_get_json(self._group_by_name_url + groupname, source="get_group_by_name")

	def get_catalog_tags(self, entity_id):
		return self._api_get_json(self._catalog_url + entity_id + "/collaboration/tag", source="get_catalog_tags", report_error=False)

	def get_catalog_wiki(self, entity_id):
		return self._api_get_json(self._catalog_url + entity_id + "/collaboration/wiki", source="get_catalog_wiki", report_error=False)

	def get_reflection(self, reflection_id):
		return self._api_get_json(self._reflections_url + reflection_id, source="get_reflection")

	def list_reflections(self):
		return self._api_get_json(self._reflections_url, source="list_reflections")

	def list_queues(self):
		return self._api_get_json(self._wlm_queue_url, source="list_queues")

	def list_rules(self):
		return self._api_get_json(self._wlm_rule_url, source="list_rules")

	def list_votes(self):
		return self._api_get_json(self._wlm_vote_url, source="list_votes")

	# This method has to be refactored when DX-16597 is resolved
	def list_pds(self, source_filter=None, source_exclude_filter=None,
				 source_folder_filter=None, source_folder_exclude_filter=None,
				 pds_filter=None, pds_exclude_filter=None, pds_error_list=None):
		pds_list = []
		# Check filters for complete PDS suppression
		if source_filter is None or (source_exclude_filter is not None and source_exclude_filter == "*"):
			return pds_list
		# Retrieve PDS list from Dremio meta-schema
		sql = " SELECT TABLE_SCHEMA, TABLE_NAME FROM \\\"INFORMATION_SCHEMA\\\".\\\"TABLES\\\" WHERE TABLE_TYPE = 'TABLE' "

		schema_filter = None
		# Clean up source filter
		if source_filter is not None:
			source_filter = source_filter.replace("*", "%")
			# Remove leading dot
			if source_filter[0:1] == '.':
				source_filter = source_filter[1:]
			# Remove trailing dot
			if source_filter[-1:] == '.':
				source_filter = source_filter[:-1]
		# Clean up folder filter
		if source_folder_filter is not None:
			source_folder_filter = source_folder_filter.replace("*", "%")
			# Remove leading dot
			if source_folder_filter[0:1] == '.':
				source_folder_filter = source_folder_filter[1:]
			# Remove trailing dot
			if source_folder_filter[-1:] == '.':
				source_folder_filter = source_folder_filter[:-1]
		# Add dots as needed
		if source_filter is not None and source_folder_filter is not None:
			schema_filter = source_filter + '.' + source_folder_filter
		elif source_filter is None and source_folder_filter is not None:
			schema_filter = '%.' + source_folder_filter
		elif source_filter is not None and source_folder_filter is None:
			schema_filter = source_filter + '.%'
		if schema_filter is not None:
			sql = sql + " and TABLE_SCHEMA like '" + schema_filter + "'"

		schema_exclude_filter = None
		# Clean up source exclude filter
		if source_exclude_filter is not None:
			source_exclude_filter = source_exclude_filter.replace("*", "%")
			# Remove leading dot
			if source_exclude_filter[0:1] == '.':
				source_exclude_filter = source_exclude_filter[1:]
			# Remove trailing dot
			if source_exclude_filter[-1:] == '.':
				source_exclude_filter = source_exclude_filter[:-1]
		# Clean up folder exclude filter
		if source_folder_exclude_filter is not None:
			source_folder_exclude_filter = source_folder_exclude_filter.replace("*", "%")
			# Remove leading dot
			if source_folder_exclude_filter[0:1] == '.':
				source_folder_exclude_filter = source_folder_exclude_filter[1:]
			# Remove trailing dot
			if source_folder_exclude_filter[-1:] == '.':
				source_folder_exclude_filter = source_folder_exclude_filter[:-1]
		# Add dots as needed
		if source_exclude_filter is not None and source_folder_exclude_filter is not None:
			schema_exclude_filter = source_exclude_filter + '.' + source_folder_exclude_filter
		elif source_exclude_filter is None and source_folder_exclude_filter is not None:
			schema_exclude_filter = '%.' + source_folder_exclude_filter
		elif source_exclude_filter is not None and source_folder_exclude_filter is None:
			schema_exclude_filter = source_exclude_filter + '.%'
		if schema_exclude_filter is not None:
			sql = sql + " and TABLE_SCHEMA not like '" + schema_exclude_filter + "' "

		if pds_filter is not None:
			pds_filter = pds_filter.replace("*", "%")
			sql = sql + " and TABLE_NAME like '" + pds_filter + "' "
		if pds_exclude_filter is not None:
			pds_exclude_filter = pds_exclude_filter.replace("*", "%")
			sql = sql + " and TABLE_NAME not like '" + pds_exclude_filter + "' "
		jobid = self.submit_sql(sql)
		# Wait for the job to complete. Should only take a moment
		while True:
			job_info = self.get_job_info(jobid)
			logging.info("list_pds: waiting for SQL query to finish. Job status: " + job_info["jobState"])
			if job_info is None:
				logging.critical("list_pds: unexpected error. Cannot get a list of PDS.")
				raise RuntimeError("Unexpected error. Cannot get a list of PDS.")
			if job_info["jobState"] in ['CANCELED', 'FAILED']:
				logging.critical("list_pds: unexpected error, SQL job failed. Cannot get a list of PDS.")
				raise RuntimeError("Unexpected error, SQL job failed. Cannot get a list of PDS.")
			if job_info["jobState"] == 'COMPLETED':
				break
			time.sleep(1)
		# Retrieve list of PDS
		job_result = self.get_job_result(jobid)
		num_rows = int(job_result['rowCount'])
		if num_rows == 0:
			logging.warn("list_pds: no PDS found as per filter criteria.")
			return pds_list
		logging.info("list_pds: processing " + str(num_rows) + " PDSs in batches of 100.")
		# Page through the results, 100 rows per page
		limit = 100
		for i in range(0, int(num_rows / limit) + 1):
			logging.info("list_pds: processing batch " + str(i + 1))
			job_result = self.get_job_result(jobid, limit * i, limit)
			for row in job_result['rows']:
				# The schema (path) is denormalized: instead of abc/ab.c/abc it has abc.ab.c.abc, we need to recover it
				normalized_path = self._normalize_schema(row['TABLE_SCHEMA'])
				entity = self.get_catalog_entity_by_path(normalized_path + row['TABLE_NAME'])
				if entity is None:
					if pds_error_list is not None:
						pds_error_list.append({"name": row['TABLE_NAME'], "path": normalized_path})
					logging.error("list_pds: error reading entity for: " + normalized_path + row['TABLE_NAME'] + " The SOURCE is likely not available at the moment. See DEBUG logging for more information.")
				else:
					pds_list.append(entity)
		return pds_list

	def _normalize_schema(self, schema):
		path = schema.split('.')
		normalized_path = ""
		for i in range(0, len(path)):
			normalized_path = normalized_path + path[i]
			entity = self.get_catalog_entity_by_path(normalized_path, report_error=False)
			if entity is not None:
				normalized_path = normalized_path + '/'
			else:
				normalized_path = normalized_path + '.'
		return normalized_path

	def create_catalog_entity(self, entity, dry_run=True):
		if dry_run:
			logging.warn("create_catalog_entity: Dry Run. Not submitting changes to API.")
			return
		return self._api_post_json(self._catalog_url, entity, source="create_catalog_entity")

	def update_catalog_entity(self, entity_id, entity, dry_run=True):
		if dry_run:
			logging.warn("update_catalog_entity: Dry Run. Not submitting changes to API.")
			return
		return self._api_put_json(self._catalog_url + entity_id, entity, source="update_catalog_entity")

	def create_reflection(self, reflection, dry_run=True):
		if dry_run:
			logging.warn("create_reflection: Dry Run. Not submitting changes to API.")
			return
		return self._api_post_json(self._reflections_url, reflection, source="create_reflection")

	def update_reflection(self, reflection_id, reflection, dry_run=True):
		if dry_run:
			logging.warn("update_reflection: Dry Run. Not submitting changes to API.")
			return
		return self._api_put_json(self._reflections_url + reflection_id, reflection, source="update_reflection")

	def promote_pds(self, pds_entity, dry_run=True):
		if dry_run:
			logging.warn("promote_pds: Dry Run. Not submitting changes to API.")
			return
		return self._api_post_json(self._catalog_url + self._encode_http_param(pds_entity['id']), pds_entity,
								   source="promote_pds")

	# Returns Job ID or None
	def submit_sql(self, sql, context=None):
		payload = '{ "sql":"' + sql + '"' + ("" if context is None else ',{"context":"' + context + '"') + ' }'
		jsn = self._api_post_json(self._post_sql_url, payload, source="submit_sql", as_json=False)
		if jsn is not None:
			return jsn["id"]
		else:
			return None

	def get_job_info(self, jobid):
		return self._api_get_json(self._get_job_url + jobid, source="get_job_info")

	def get_job_result(self, jobid, offset=0, limit=100):
		return self._api_get_json(self._get_job_url + jobid + '/results?offset=' + str(offset) + '&limit=' + str(limit),
								  source="get_job_info")

	# Returns JSON if success or None
	def _api_get_json(self, url, source="", report_error=True):
		# Extract source
		source_name = None
		pos = url.find(self._catalog_url_by_path)
		if pos >= 0:
			source_name = url[pos + 23:]
			source_name = source_name[0:source_name.find("/")]
		else:
			pos = url.find(self._catalog_url)
			if pos >= 0:
				source_name = url[pos + 23:]
				source_name = source_name[0:source_name.find("/")]

		try:
			if source_name in self._timed_out_sources and not self._retry_timedout_source:
				raise requests.exceptions.Timeout()
			response = requests.request("GET", self._endpoint + url, headers=self._headers, timeout=self._api_timeout, verify=self._verify_ssl)
			if response.status_code == 200:
				return response.json()
			elif response.status_code == 400:  # Bad Request
				if report_error:
					logging.info(source + ": received HTTP Response Code " + str(response.status_code) +
									" for : <" + str(url) + ">" + self._get_error_message(response))
			elif response.status_code == 404:  # Not found
				if report_error:
					logging.info(source + ": received HTTP Response Code " + str(response.status_code) +
									" for : <" + str(url) + ">" + self._get_error_message(response))
			else:
				if report_error:
					logging.error(source + ": received HTTP Response Code " + str(response.status_code) +
								" for : <" + str(url) + ">" + self._get_error_message(response))
					self.errors_encountered = self.errors_encountered + 1
			return None
		except requests.exceptions.Timeout:
			if source_name is None or source_name not in self._timed_out_sources:
				# This situation might happen when an underlying object (file system eg) is not responding
				if report_error:
					logging.error(source + ": HTTP Request Timed-out: " + " <" + str(url) + ">")
					self.errors_encountered = self.errors_encountered + 1
				else:
					logging.info(source + ": HTTP Request Timed-out: " + " <" + str(url) + ">")
			if source_name is not None and source_name not in self._timed_out_sources:
				self._timed_out_sources.append(source_name)
			return None

	# Returns JSON if success or None
	def _api_post_json(self, url, json_data, source="", as_json=True):
		try:
			if as_json:
				response = requests.request("POST", self._endpoint + url, json=json_data, headers=self._headers, timeout=self._api_timeout, verify=self._verify_ssl)
			else:
				response = requests.request("POST", self._endpoint + url, data=json_data, headers=self._headers, timeout=self._api_timeout, verify=self._verify_ssl)
			if response.status_code == 200:
				return response.json()
			elif response.status_code == 400:
				logging.error(source + ": received HTTP Response Code " + str(response.status_code) +
							  " for : <" + str(url) + ">" + self._get_error_message(response))
			elif response.status_code == 403:  # User does not have permission
				logging.critical(source + ": received HTTP Response Code " + str(response.status_code) +
								 " for : <" + str(url) + ">" + self._get_error_message(response))
				raise RuntimeError(
					"Specified user does not have sufficient priviliges to create objects in the target Dremio Environment.")
			elif response.status_code == 409:  # Already exists.
				logging.error(source + ": received HTTP Response Code " + str(response.status_code) +
							  " for : <" + str(url) + ">" + self._get_error_message(response))
			elif response.status_code == 404:  # Not found
				logging.info(source + ": received HTTP Response Code " + str(response.status_code) +
							 " for : <" + str(url) + ">" + self._get_error_message(response))
			else:
				logging.error(source + ": received HTTP Response Code " + str(response.status_code) +
							  " for : <" + str(url) + ">" + self._get_error_message(response))
				self.errors_encountered = self.errors_encountered + 1
			return None
		except requests.exceptions.Timeout:
			# This situation might happen when an underlying object (file system eg) is not responding
			logging.error(source + ": HTTP Request Timed-out: " + " <" + str(url) + ">")
			self.errors_encountered = self.errors_encountered + 1
			return None

	# Returns JSON if success or None
	def _api_put_json(self, url, json_data, source=""):
		try:
			response = requests.request("PUT", self._endpoint + url, json=json_data, headers=self._headers, timeout=self._api_timeout, verify=self._verify_ssl)
			if response.status_code == 200:
				return response.json()
			elif response.status_code == 400:  # The supplied CatalogEntity object is invalid.
				logging.error(source + ": received HTTP Response Code 400 for : <" + str(url) + ">" +
							  self._get_error_message(response))
			elif response.status_code == 403:  # User does not have permission to create the catalog entity.
				logging.critical(source + ": received HTTP Response Code 403 for : <" + str(url) + ">" +
								 self._get_error_message(response))
				raise RuntimeError(
					"Specified user does not have sufficient priviliges to create objects in the target Dremio Environment.")
			elif response.status_code == 409:  # A catalog entity with the specified path already exists.
				logging.error(source + ": received HTTP Response Code 409 for : <" + str(url) + ">" +
							  self._get_error_message(response))
			elif response.status_code == 404:  # Not found
				logging.info(source + ": received HTTP Response Code 404 for : <" + str(url) + ">" +
							 self._get_error_message(response))
			else:
				logging.error(source + ": received HTTP Response Code " + str(response.status_code) +
							  " for : <" + str(url) + ">" + self._get_error_message(response))
				self.errors_encountered = self.errors_encountered + 1
			return None
		except requests.exceptions.Timeout:
			# This situation might happen when an underlying object (file system eg) is not responding
			logging.error(source + ": HTTP Request Timed-out: " + " <" + str(url) + ">")
			self.errors_encountered = self.errors_encountered + 1
			return None

	def _get_error_message(self, response):
		message = ""
		try:
			if 'errorMessage' in response.json():
				message = message + " errorMessage: " + str(response.json()['errorMessage'])
			if 'moreInfo' in response.json():
				message = message + " moreInfo: " + str(response.json()['moreInfo'])
		except:
			message = message + " content: " + str(response.content)
		return message

	def _encode_http_param(self, path):
		# urllib.quote_plus=urllib.quote # A fix for urlencoder to give %20
		return urllib.quote_plus(path)
