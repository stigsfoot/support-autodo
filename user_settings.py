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


"""User settings page for the application."""



import os
import re
from apiclient.discovery import build
from google.appengine.api import users
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import login_required
import httplib2
import model
import settings


class UserSettingsPage(webapp.RequestHandler):
  """RequestHandler for the Admin page."""

  @login_required
  def get(self):
    """Display the admin page template."""
    template_value = {}

    user = users.get_current_user()
    template_value['current_user'] = user.email()
    if users.is_current_user_admin():
      credentials = model.Credentials.get_by_key_name(
          settings.CREDENTIALS_KEYNAME)
      if credentials:
        template_value['credentials_email'] = credentials.email
      template_value['is_admin'] = True
    else:
      template_value['is_admin'] = False

    # Determine whether or not the server is running locally, and offer a
    # datastore reset if it's not.
    if re.search('(appspot)', self.request.host):
      template_value['is_local'] = False
    else:
      template_value['is_local'] = True

    # Make a list of tags from the datastore to pass to template.
    suggestion_models = model.SuggestionModel.all()
    suggestion_models.order('__key__')
    template_value['models'] = suggestion_models

    credentials = model.Credentials.get_by_key_name(
        settings.CREDENTIALS_KEYNAME)
    status = {}

    if credentials is not None:
      credentials = credentials.credentials
      http = httplib2.Http()
      http = credentials.authorize(http)
      service = build('prediction', 'v1.2', http=http)
      train = service.training()

      for suggestion_model in suggestion_models:
        gs_full_name = '%s/%s' % (settings.GS_BUCKET,
                                  suggestion_model.training_file)
        state = train.get(data=gs_full_name).execute()
        status[suggestion_model.name] = state['trainingStatus']
    else:
      status['Add Credentials to access models'] = '...'

    template_value['status'] = status
    path = os.path.join(settings.TEMPLATE_BASE_PATH, 'user_settings.html')
    self.response.out.write(template.render(path, template_value))
