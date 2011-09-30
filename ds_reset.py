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


"""Datastore reset page for the application."""



from datetime import datetime
import re
from google.appengine.api import users
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import model


class DatastoreResetPage(webapp.RequestHandler):
  """Handler for datastore reset requests."""

  def post(self):
    """Resets the datastore."""
    # Ignore if not local.
    if not re.search('(appspot)', self.request.host):
      for incident in model.Incident.all():
        incident.delete()

      for message in model.Message.all():
        message.delete()

      user = users.get_current_user()

      # Create an incident for the user.
      self.CreateIncident('Incident for ' + user.nickname(), user.nickname())

      # Creates an unassigned incident.
      self.CreateIncident('Unassigned incident')

      # Creates an incident assigned to 'some_user' if one doesn't exist.
      if user.nickname() is not 'some_user':
        self.CreateIncident('Incident for some_user', 'some_user')

      # Creates an incident with the accepted tag of 'API-Test'.
      self.CreateIncident('API-Test', 'none', ['API-Test'])

      # Creates an incident with the accepted tag of 'Special-ToAssignTag'.
      self.CreateIncident('To assign tag', 'none', ['Special-ToAssignTag'])

  def CreateIncident(self, title, owner='none', accepted_tags=None,
                     suggested_tags=None):
    """Creates an incident with limited customization.

    Args:
      title: Title of the incident
      owner: Optionally specifies the owner of the incident.
      accepted_tags: Optional list of accepted_tags applied to the incident.
      suggested_tags: Optional list of suggested_tags applied to the incident.
    """
    # Set empty tags outside of the default constructor, in case we ever need
    # to modify these later.
    if not accepted_tags:
      accepted_tags = []
    if not suggested_tags:
      suggested_tags = []

    incident = model.Incident()
    incident.title = title
    incident.created = datetime.now()
    incident.status = 'NEW'
    incident.owner = owner
    incident.author = 'test@example.com'
    incident.mailing_list = 'support@example.com'
    incident.canonical_link = 'http://google.com'
    incident.suggested_tags = suggested_tags
    incident.accepted_tags = accepted_tags
    incident.put()
    self.CreateMessages(incident)

  def CreateMessages(self, incident):
    """Creates messages associated with the supplied incident.

    Args:
      incident: Incident to which messages should be appended.
    """
    in_reply_to = None
    for j in range(2):
      message = model.Message()
      message.title = 'Message #' + str(j)
      message.incident = incident
      message.in_reply_to = in_reply_to
      message.message_id = 'message-%s-%s' % (incident.key, str(j))
      message.author = 'text@example.com'
      message.body = 'Text'
      message.sent = datetime.now()
      message.mailing_list = 'support@example.com'
      message.canonical_link = 'http://google.com'
      message.put()
      in_reply_to = message.message_id


application = webapp.WSGIApplication(
    [
        ('/ds_reset', DatastoreResetPage)
    ],
    debug=True)


def main():
  """Runs the application."""
  run_wsgi_app(application)


if __name__ == '__main__':
  main()
