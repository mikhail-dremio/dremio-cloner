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

import fnmatch, re
from DremioClonerUtils import DremioClonerUtils
from DremioClonerLogger import DremioClonerLogger

class DremioClonerFilter():

	_config = None
	_utils = None
	_logger = None

	def __init__(self, config):
		self._config = config
		self._logger = DremioClonerLogger(self._config.max_errors, self._config.logging_verbose)
		self._utils = DremioClonerUtils(config)

	def match_space_filter(self, container, loginfo = False):
		if self._match_path(self._config._space_filter_re, self._config._space_exclude_filter_re, None, None, None, None, container):
			return True
		if loginfo:
			self._logger.info("match_space_filter: skipping SPACE " + container['path'][0] if 'path' in container else container['name'] + " as per job configuration")
		return False

	def match_space_folder_filter(self, container, loginfo = True):
		if self._match_path(self._config._space_filter_re, self._config._space_exclude_filter_re, self._config._space_folder_filter_re, self._config._space_folder_exclude_filter_re, None, None, container):
			return True
		if loginfo:
			self._logger.debug("match_space_folder_filter: skipping SPACE FOLDER " + container['path'][0] if 'path' in container else container['name'] + " as per job configuration")
		return False

	def match_space_folder_cascade_acl_origin_filter(self, container):
		if self._config.space_folder_cascade_acl_origin_filter is None:
			return False
		elif (  # Do not filter out folders in HOME hierarchies
				(container['path'][0][:1] == '@') or
				# Match both Folder filter and Space filter
				((self._config._space_folder_cascade_acl_origin_filter_re.match(self._utils.normalize_path(container['path'][1:])) is not None) and
				 self.match_space_filter(container)) ):
			return True
		else:
			return False

	def match_source_filter(self, container, loginfo = True):
		if self._match_path(self._config._source_filter_re, self._config._source_exclude_filter_re, None, None, None, None, container):
			return True
		if loginfo:
			self._logger.debug("match_source_filter: skipping SOURCE " + container['path'][0] if 'path' in container else container['name'] + " as per job configuration")
		return False

	def match_source_folder_filter(self, container, loginfo = True):
		if self._match_path(self._config._source_filter_re, self._config._source_exclude_filter_re, self._config._source_folder_filter_re, self._config._source_folder_exclude_filter_re, None, None, container):
			return True
		if loginfo:
			self._logger.debug("match_source_folder_filter: skipping SOURCE FOLDER " + container['path'][0] if 'path' in container else container['name'] + " as per job configuration")
		return False

	def match_pds_filter(self, pds, loginfo = True):
		if self._match_path(self._config._source_filter_re, self._config._source_exclude_filter_re, self._config._source_folder_filter_re, self._config._source_folder_exclude_filter_re, self._config._pds_filter_re, self._config.pds_exclude_filter, pds):
			return True
		if loginfo:
			self._logger.debug("match_pds_filter: skipping PDS " + pds['path'][-1] if 'path' in pds else pds['name'] + " as per job configuration")
		return False

	def match_vds_filter(self, vds, loginfo = True):
		if self._match_path(self._config._space_filter_re, self._config._space_exclude_filter_re, self._config._space_folder_filter_re, self._config._space_folder_exclude_filter_re, self._config._vds_filter_re, self._config.vds_exclude_filter_re, vds):
			return True
		if loginfo:
			self._logger.debug("match_vds_filter: skipping VDS " + vds['path'][-1] if 'path' in vds else vds['name'] + " as per job configuration")
		return False


	def _match_path(self, root_re, root_exclusion_re, folder_re, folder_exclusion_re, object_re, object_exclusion_re, entity):
		# If inclusion filter is not specified, nothing to process
		if root_re is None:
			return False
		# Validate parameters
		if ('containerType' in entity and entity['containerType'] == 'SPACE') or \
		   ('entityType' in entity and entity['entityType'] == 'space') or \
		   ('containerType' in entity and entity['containerType'] == 'SOURCE') or \
		   ('entityType' in entity and entity['entityType'] == 'source')	:
			pass
		elif ('entityType' in entity and entity['entityType'] == 'folder') or \
				('containerType' in entity and entity['containerType'] == 'FOLDER'):
			if root_re is None or folder_re is None:
				return False
		elif ('entityType' in entity and entity['entityType'] == 'dataset') or \
				('type' in entity and entity['type'] == 'DATASET'):
			if root_re is None or folder_re is None or object_re is None:
				return False
		else:
			self._logger.fatal("_match_path: Unexpected Entity Type " + str(entity))
		if 'path' not in entity:
			return root_exclusion_re is None or root_exclusion_re.match(entity['name'])
		else:
			path = entity['path']
			# Match root object (Space of Source)
			if root_re.match(path[0]) is None:
				return False
			if root_exclusion_re is not None and root_exclusion_re.match(path[0]) is None:
				return False
			# Match object
			if object_re is not None and object_re.match(self._utils.normalize_path(path[-1])) is None:
				return False
			if object_exclusion_re is not None and object_exclusion_re.match(self._utils.normalize_path(path[1:])) is not None:
				return False
			# Match Folders. Note, child folders do not need to be matched if its parent match
			if folder_re is not None or folder_exclusion_re is not None:
				folder_matched = False
				for i in range(len(path)):
					if folder_re.match(self._utils.normalize_path(path[1:len(path) - i])) is not None:
						folder_matched = True
						break
				if not folder_matched:
					return False
				if folder_exclusion_re is not None:
					folder_exclusion_matched = False
					for i in range(len(path)):
						if folder_exclusion_re.match(self._utils.normalize_path(path[1:len(path) - i])) is not None:
							folder_exclusion_matched = True
							break
					if folder_exclusion_matched:
						return False
		return True

