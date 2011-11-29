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


"""Authorization grant page for the application."""



import logging
import os
import pickle
from oauth2client.appengine import StorageByKeyName
from oauth2client.client import OAuth2WebServerFlow
from google.appengine.api import memcache
from google.appengine.api import users
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import login_required
from google.appengine.ext.webapp.util import run_wsgi_app
import model
import settings


"""
Maps OAuth API parameter with API scope.
The current supported values are:
{
  '<api_name>': {
     'admin_required': Whether or not this API is "admin-only".
     'scopes': The requested Google API Scopes.
     'model': Datastore model used to store the credentials.
     'credentials_attribute': Datastore model attribute used to store the
                              credentials.
     'key_name': Key name to use if only one instance of this model has to be
                 stored at a time. Optional, this value default to the current
                 user ID.
"""
SCOPES = {
    'prediction': {
        'admin_required': True,
        'scopes': ['https://www.googleapis.com/auth/prediction'],
        'model': model.Credentials,
        'credentials_attribute': 'credentials',
        'key_name': settings.CREDENTIALS_KEYNAME
    },
    'tasks': {
        'admin_required': False,
        'scopes': ['https://www.googleapis.com/auth/tasks'],
        'model': model.UserSettings,
        'credentials_attribute': 'tasks_credentials'
    }
}


class OAuthGrantPage(webapp.RequestHandler):
  """RequestHandler for the authorization grant page."""

  @login_required
  def get(self, api):
    """Handle the GET request for the OAuth grant page.

    Construct the authorization grant URL and redirect the user to it.

    Args:
      api: Private API name to ask access for (should be a key of SCOPES).
    """
    if (api not in SCOPES or
        SCOPES[api]['admin_required'] and not users.is_current_user_admin()):
      self.status(400)
    else:
      user = users.get_current_user()
      logging.info('%s (%s) has entered OAuth 2.0 grant flow',
                   user.email(), user.user_id())
      flow = OAuth2WebServerFlow(client_id=settings.CLIENT_ID,
                                 client_secret=settings.CLIENT_SECRET,
                                 scope=' '.join(SCOPES[api]['scopes']),
                                 user_agent=settings.USER_AGENT,
                                 domain=settings.DOMAIN,
                                 state=api)
      callback = self.request.host_url + '/oauth2callback'
      authorize_url = flow.step1_get_authorize_url(callback)

      memcache.set(user.user_id() + api, pickle.dumps(flow))
      self.redirect(authorize_url)


class OAuthCallbackPage(webapp.RequestHandler):
  """RequestHandler for the authorization callback page."""

  @login_required
  def get(self):
    """Handle the GET request for the OAuth callback page.

    Get the stored user's credentials flow and request the access token to
    finish the OAuth 2.0 dance.
    If successful, the user's OAuth 2.0 credentials are stored in the datastore.
    """
    user = users.get_current_user()
    error = self.request.get('error')
    api = self.request.params.get('state')
    if (api not in SCOPES or
        SCOPES[api]['admin_required'] and not users.is_current_user_admin()):
      self.status(404)
    elif error and error == 'access_denied':
      logging.warning('%s (%s) has denied access to the APIs',
                      user.email(), user.user_id())
    else:
      pickled_flow = memcache.get(user.user_id() + api)
      if pickled_flow:
        flow = pickle.loads(pickled_flow)
        credentials = flow.step2_exchange(self.request.params)
        StorageByKeyName(
            SCOPES[api]['model'], SCOPES[api].get('key_name') or user.email(),
            SCOPES[api]['credentials_attribute']).put(credentials)
        if SCOPES[api].get('key_name'):
          # Add the email to the datastore Credentials entry.
          credentials = model.Credentials.get_by_key_name(
              settings.CREDENTIALS_KEYNAME)
          credentials.email = user.email()
          credentials.put()
        logging.info('Successfully stored OAuth 2.0 credentials for: %s (%s)',
                     user.email(), user.user_id())
      else:
        logging.warning('Unknown flow for user: %s (%s)',
                        user.email(), user.user_id())
        self.redirect('/')
    path = os.path.join(settings.TEMPLATE_BASE_PATH, 'oauth.html')
    self.response.out.write(template.render(path, {}))


application = webapp.WSGIApplication(
    [
        ('/oauth/(.*)', OAuthGrantPage),
        ('/oauth2callback', OAuthCallbackPage),
    ],
    debug=True)


def main():
  """Runs the application."""
  run_wsgi_app(application)


if __name__ == '__main__':
  main()
