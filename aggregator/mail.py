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


"""Aggregates individual email messages into a single incident."""



import logging
from time import strftime
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util
from google.appengine.ext.webapp.mail_handlers import InboundMailHandler
import model


class MailAggregator(InboundMailHandler):
  """Handles incoming mail where each message is delivered individually."""

  # Whether or not to save the message body on initial save.
  SAVE_FULL_TEXT = True

  FAKE_MESSAGE_ID = 'FAKEMESSAGEID'
  # Format suitable for strftime
  FAKE_MESSAGE_ID_SUFFIX_FORMAT = '%Y%m%d%H%M%S'

  def receive(self, mail):
    """Handles receipt of an email message.

    Args:
      mail: Incoming message to parse.
    """
    # Check for delivery dupes, first.
    message_id = mail.original.get('Message-ID')
    if message_id is None:
      message_id = MailAggregator.FAKE_MESSAGE_ID + strftime(
          MailAggregator.FAKE_MESSAGE_ID_SUFFIX_FORMAT)

    message = model.Message.gql('WHERE message_id = :1', message_id).get()

    # If there isn't already a copy, save the email.
    if not message:
      message = model.Message.FromMail(mail, message_id,
                                       MailAggregator.SAVE_FULL_TEXT)

    # Incident association is idempotent and can be repeated.
    message.AssociateMailIncident()


def main():
  logging.getLogger().setLevel(logging.DEBUG)
  application = webapp.WSGIApplication([MailAggregator.mapping()], debug=True)

  util.run_wsgi_app(application)


if __name__ == '__main__':
  main()
