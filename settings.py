#!/usr/bin/env python
#
# Copyright 2011 Google Inc.
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


"""Settings for the Au-to-do app."""




TEMPLATE_BASE_PATH = 'templates/'

# Prediction API credentials keyname.
CREDENTIALS_KEYNAME = 'prediction_credentials'

# OAuth 2.0 related constant.
CLIENT_ID = (
    'your_client_id'
)
CLIENT_SECRET = 'your_client_secret'
# TODO(user): Make sure that all the scopes are included.
SCOPES = ['https://www.googleapis.com/auth/prediction']
USER_AGENT = 'au-to-do'
DOMAIN = 'anonymous'

# Whether or not to use memcache for caching of JSON models.
USE_MEMCACHE_FOR_JSON_MODELS = True
MEMCACHE_VERSION_PREFIX = '1-'

# Google Storage Legacy Access
GS_LEGACY_ACCESS = 'your_legacy_access_key'
GS_LEGACY_SECRET = 'your_legacy_access_secret'
GS_BUCKET = 'autodo-predictionmodels'
