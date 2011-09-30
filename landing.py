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


"""Landing page for the application."""



import os
from google.appengine.api import users
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from api.handler import INCIDENT_FILTERS
from settings import TEMPLATE_BASE_PATH


class LandingPage(webapp.RequestHandler):
  """Landing page handler."""

  def get(self):
    """Render the landing page."""
    user = users.get_current_user()
    if user:
      owner = user.email().split('@')[0]
      template_values = {
          'owner': owner,
          # Retrieve the list of filters to add as autocomplete params.
          'filters': [x[0] for x in INCIDENT_FILTERS],
      }
      path = os.path.join(TEMPLATE_BASE_PATH, 'page.html')
      self.response.out.write(template.render(path, template_values))
    else:
      self.redirect(users.create_login_url(self.request.uri))
