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


class OAuthGrantPage(webapp.RequestHandler):
  """RequestHandler for the authorization grant page."""

  @login_required
  def get(self):
    """Handle the GET request for the OAuth grant page.

    Construct the authorization grant URL and redirect the user to it.
    """
    user = users.get_current_user()
    logging.info('%s (%s) has entered OAuth 2.0 grant flow',
                 user.email(), user.user_id())
    flow = OAuth2WebServerFlow(client_id=settings.CLIENT_ID,
                               client_secret=settings.CLIENT_SECRET,
                               scope=' '.join(settings.SCOPES),
                               user_agent=settings.USER_AGENT,
                               domain=settings.DOMAIN)
    callback = self.request.host_url + '/oauth2callback'
    authorize_url = flow.step1_get_authorize_url(callback)

    memcache.set(user.user_id(), pickle.dumps(flow))
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
    if error and error == 'access_denied':
      logging.warning('%s (%s) has denied access to the APIs',
                      user.email(), user.user_id())
    else:
      pickled_flow = memcache.get(user.user_id())
      if pickled_flow:
        flow = pickle.loads(pickled_flow)
        credentials = flow.step2_exchange(self.request.params)
        # We are only storing one set of credentials at a time.
        StorageByKeyName(model.Credentials, settings.CREDENTIALS_KEYNAME,
                         'credentials').put(credentials)
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
        ('/oauth', OAuthGrantPage),
        ('/oauth2callback', OAuthCallbackPage),
    ],
    debug=True)


def main():
  """Runs the application."""
  run_wsgi_app(application)


if __name__ == '__main__':
  main()
