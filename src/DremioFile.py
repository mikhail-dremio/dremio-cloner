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

from DremioData import DremioData
from DremioClonerConfig import DremioClonerConfig
from datetime import datetime
import json
import logging
import os, errno

class DremioFile():

	@staticmethod
	def save_dremio_environment(dremio_config, dremio_data, filename):
		f = open(filename, "w")
		f.write('{ "data": [')
		json.dump({'dremio_environment': [{'file_version':'0.3'},{'base_url':dremio_config.source_endpoint},{'timestamp_utc':str(datetime.utcnow())}]}, f)
		f.write(',')
		# Remove password if present
		for config_item in dremio_config.cloner_conf_json:
			if 'source' in config_item:
				for source_item in config_item['source']:
					if 'password' in source_item:
						source_item['password'] = ''
						break
		json.dump({'dremio_get_config':dremio_config.cloner_conf_json}, f)
		f.write(',')
		json.dump({'containers':dremio_data.containers}, f)
		f.write(',')
		json.dump({'homes':dremio_data.homes}, f)
		f.write(',')
		json.dump({'sources':dremio_data.sources}, f)
		f.write(',')
		json.dump({'spaces':dremio_data.spaces}, f)
		f.write(',')
		json.dump({'folders':dremio_data.folders}, f)
		f.write(',')
		json.dump({'pds':dremio_data.pds_list}, f)
		f.write(',')
		json.dump({'vds':dremio_data.vds_list}, f)
		f.write(',')
		json.dump({'files':dremio_data.files}, f)
		f.write(',')
		json.dump({'reflections':dremio_data.reflections}, f)
		f.write(',')
		json.dump({'referenced_users':dremio_data.referenced_users}, f)
		f.write(',')
		json.dump({'referenced_groups':dremio_data.referenced_groups}, f)
		f.write(',')
		json.dump({'queues':dremio_data.queues}, f)
		f.write(',')
		json.dump({'rules':dremio_data.rules}, f)
		f.write(',')
		json.dump({'tags':dremio_data.tags}, f)
		f.write(',')
		json.dump({'wikis':dremio_data.wikis}, f)
		f.write(',')
		json.dump({'votes':dremio_data.votes}, f)
		# TODO
		#if dremio_data.vds_parents:
		#	f.write(',')
		#	json.dump({'vds_parents':str(dremio_data.vds_parents)}, f)
		f.write(' ] }')
		f.close()

	@staticmethod
	def read_dremio_environment(filename):
		f = open(filename, "r")
		data = json.load(f)['data']
		f.close()
		dremio_data = DremioData()
		for item in data:
			if ('dremio_environment' in item):
				logging.info("read_dremio_environment: processing environment " + str(item))
			elif ('containers' in item):
				dremio_data.containers = item['containers']
			elif ('homes' in item):
				dremio_data.homes = item['homes']
			elif ('sources' in item):
				dremio_data.sources = item['sources']
			elif ('spaces' in item):
				dremio_data.spaces = item['spaces']
			elif ('folders' in item):
				dremio_data.folders = item['folders']
			elif ('pds' in item):
				dremio_data.pds_list = item['pds']
			elif ('vds' in item):
				dremio_data.vds_list = item['vds']
			elif ('files' in item):
				dremio_data.files = item['files']
			elif ('reflections' in item):
				dremio_data.reflections = item['reflections']
			elif ('referenced_users' in item):
				dremio_data.referenced_users = item['referenced_users']
			elif ('referenced_groups' in item):
				dremio_data.referenced_groups = item['referenced_groups']
			elif ('queues' in item):
				dremio_data.queues = item['queues']
			elif ('rules' in item):
				dremio_data.rules = item['rules']
			elif ('tags' in item):
				dremio_data.tags = item['tags']
			elif ('wikis' in item):
				dremio_data.wikis = item['wikis']
			elif ('votes' in item):
				dremio_data.votes = item['votes']
			elif ('vds_parents' in item):
				dremio_data.vds_parents = item['vds_parents']
			elif ('dremio_get_config' in item):
				dremio_data.get_config = item['dremio_get_config']
			else:
				logging.warn("read_dremio_environment: unexpected data in the source file " + str(item))
		return dremio_data


	@staticmethod
	def save_vds_files(dremio_config, dremio_data, target_directory):
		for vds in dremio_data.vds_list:
			DremioFile.write_vds_file(vds, target_directory)


	@staticmethod
	def write_vds_file(vds, target_directory):
		path = vds['path']
		filepath = target_directory
		for item in path:
			if not os.path.isdir(filepath):
				try:
					os.makedirs(filepath)
				except OSError as e:
					if e.errno != errno.EEXIST:
						raise "Cannot create directory: " + filepath
			filepath = filepath + "/" + item
		f = open(filepath + ".json", "w")
		json.dump(vds, f)
		f.close()
