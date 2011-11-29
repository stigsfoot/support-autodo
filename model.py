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


"""Provides models for Au-to-do data types.

This module provides data types for Au-to-do data stored in the App Engine
datastore.
"""



from datetime import datetime
from datetime import timedelta
import email.utils
import logging
import os
import re
from sets import Set
import urllib
from oauth2client.appengine import CredentialsProperty
from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.ext import db
import simplejson
import settings


INCIDENT_DEEP_LINK = 'http://%s/#id=' % os.environ.get('HTTP_HOST', 'localhost')


class Incident(db.Model):
  """Describes an incident.

  Incidents are support inquiries of one of several types. Examples include:
  a thread from a mailing list (or Google Group), a Unify ticket, or a Google
  Code Project Hosting issue.

  Attributes:
    title: Title of the incident.
    created: When the incident started (or was first tracked).
    updated: When the incident was last updated.
    resolved: When the incident was resolved.
    status: Current status of the incident (eg. waiting for customer response).
    owner: Owner of the incident.
    author: Person who created the incident.
    mailing_list: Mailing list to which the incident was sent (if from a
        mailing list).
    canonical_link: Reference to the canonical location of the incident, e.g. a
        the Google Group page or the Unify ticket.
    suggested_tags: List of tags suggested by the Prediction API/suggester.
    accepted_tags: List of tags approved or added by the user.
    trained_tags: List of accepted_tags that generated training
        examples. Used to detect changes in accepted_tags by the user.
    trained_date: Date when this Incident was last processed for training data.
    training_review: True when the training algorithm should check
        this Incident for new Accepted tags or other changes related
        to the Prediction API.
  """
  title = db.StringProperty(multiline=True)
  created = db.DateTimeProperty()
  updated = db.DateTimeProperty()
  resolved = db.DateTimeProperty()
  status = db.StringProperty(default='new')
  owner = db.StringProperty(default='none')
  author = db.StringProperty()
  mailing_list = db.StringProperty()
  canonical_link = db.StringProperty()
  suggested_tags = db.ListProperty(str)
  accepted_tags = db.ListProperty(str)
  trained_tags = db.ListProperty(str)
  trained_date = db.DateTimeProperty()
  training_review = db.BooleanProperty(default=True)

  # Format used by the class for parsing dates.
  ISO_FORMAT = '%Y-%m-%dT%H:%M:%S'

  @staticmethod
  def MergeWithParent(parent_incident, children):
    """Merges multiple child incidents into one incident with a common parent.

    When child messages are delivered prior to their parent message (eg.
    In-Reply-To references a message that has not yet been delivered), the
    children will be roots of new incidents. Once the parent message is
    delivered, this method will remove the child incidents and place them under
    the common parent.

    Records the current time in parent_incident.updated to help with
    debounce of user input.  Training may choose to ignore Incidents
    which have been modified too recently because they are possibly
    still under user review and editing.

    Sets parent_incident.training_review to True, assuming that the
    MergeWithParent might have affected the tags.

    Args:
      parent_incident: Parent incident, referenced by each of the children.
      children: One or more messages referencing defunct parents.
    """
    incidents_to_merge = Set()
    for child in children:
      incidents_to_merge.add(child.incident)
      child.incident = parent_incident
      child.put()

    parent_incident.PurgeJsonCache()
    parent_incident.updated = datetime.utcnow()
    parent_incident.training_review = True

    for incident in incidents_to_merge:
      parent_incident.accepted_tags.extend(incident.accepted_tags)
      parent_incident.suggested_tags.extend(incident.suggested_tags)
      parent_incident.trained_tags.extend(incident.trained_tags)
      parent_incident.trained_date = max([incident.trained_date,
                                          incident.trained_date])
      messages = Message.gql('WHERE incident = :1', incident.key())
      for message in messages:
        message.incident = parent_incident
        message.put()

      if not parent_incident.key() == incident.key():
        incident.delete()

    parent_incident.accepted_tags = list(set(parent_incident.accepted_tags))
    parent_incident.suggested_tags = list(set(parent_incident.suggested_tags))
    parent_incident.trained_tags = list(set(parent_incident.suggested_tags))
    parent_incident.put()

  def Overlay(self, other):
    """Overwrite this incident's fields with other incident's fields.

    Records current time as time of update (incident.updated).
    Does not overwrite messages.

    Args:
      other: Incident from which to pull values.
    """
    self.title = other.title
    self.owner = other.owner
    self.status = other.status
    self.created = other.created
    self.updated = datetime.utcnow()
    self.training_review = other.training_review
    self.resolved = other.resolved
    self.suggested_tags = other.suggested_tags
    self.accepted_tags = other.accepted_tags
    self.trained_tags = other.trained_tags
    self.trained_date = other.trained_date
    self.canonical_link = other.canonical_link

  def GetDict(self):
    """Return a dict representation of this incident, with messages.

    This will return a copy from memcache if it exists (and caching is
    enabled), and update the cache if not present.

    Returns:
      Dict representing the incident.
    """
    # Check for memcached copy first.
    key = self.GetJsonModelKey()
    cached = memcache.get(key)
    if cached and settings.USE_MEMCACHE_FOR_JSON_MODELS:
      return cached

    model = {
        'title': self.title,
        'owner': self.owner,
        'status': self.status,
        'created': self.created,
        'updated': self.updated,
        'resolved': self.resolved,
        'suggested_tags': self.suggested_tags,
        'accepted_tags': self.accepted_tags,
        'trained_tags': self.trained_tags,
        'trained_date': self.trained_date,
        'training_review': self.training_review,
        'canonical_link': self.canonical_link,
        'messages': []}
    if self.key():
      model['id'] = self.key().id()
    if self.message_set is not None:
      for m in self.message_set:
        model['messages'].append(m.GetDict())
    memcache.set(key, model)
    return model

  def PurgeJsonCache(self):
    """Purges the cached JSON representation of the incident."""
    key = self.GetJsonModelKey()
    logging.info('Purging cache for incident:' + key)
    memcache.delete(key)

  def GetJsonModelKey(self):
    """Returns the key pointing to the instance's JSON representation.

    Returns:
      JSON model key.
    """
    return settings.MEMCACHE_VERSION_PREFIX + str(self.key().id())

  def ToTaskDict(self, body=None):
    """Parse an incident into a Tasks API dictionary.

    Args:
      body: Optional dictionary to update.
    Returns:
      Dictionary representing the incident.
    """
    body = body or {}
    body['title'] = self.title
    body['notes'] = self.GetDeepLink()
    if self.resolved:
      body['status'] = 'completed'
      body['completed'] = self.GetDateTime(self.resolved)
    else:
      body['status'] = 'needsAction'
      if 'completed' in body:
        body.pop('completed')
    return body

  def GetDeepLink(self):
    """Return a deeplink to the incident.

    Returns:
      Deeplink to the incident.
    """
    return '%s%s' % (INCIDENT_DEEP_LINK, self.key().id())

  @staticmethod
  def FromJson(json):
    """Convert the given JSON representation to an Incident.

    Sets 'incident.training_review' to True, assuming that anything
    could have changed on the client which sent the JSON, including
    the Tags.

    Does not include messages from given JSON, as messages are read-only.

    Args:
      json: JSON representation to convert.
    Returns:
      Incident with all the properties of the given JSON representation.
    """
    retval = simplejson.loads(json)
    incident = Incident(
        title=retval.get('title'),
        owner=retval.get('owner'),
        status=retval.get('status'),
        suggested_tags=retval.get('suggested_tags'),
        accepted_tags=retval.get('accepted_tags'),
        trained_tags=retval.get('trained_tags'),
        training_review=True,
        canonical_link=retval.get('canonical_link'))
    if retval.get('created') is not None:
      incident.created = Incident.ParseDate(retval.get('created'))
    if retval.get('updated') is not None:
      incident.updated = Incident.ParseDate(retval.get('updated'))
    if retval.get('resolved') is not None:
      incident.resolved = Incident.ParseDate(retval.get('resolved'))
    if retval.get('trained_date') is not None:
      incident.trained_date = Incident.ParseDate(retval.get('trained_date'))
    return incident

  @staticmethod
  def ParseDate(date_string):
    """Converts a string into the ISO date format.

    Args:
      date_string: ISO-formatted date string.
    Returns:
      Native datetime object.
    """
    if '.' in date_string:
      (dt, microsecs) = date_string.split('.', 1)
      if len(microsecs) > 3:
        microsecs = microsecs[:3]
    else:
      dt = date_string
      microsecs = 0
    return_datetime = datetime.strptime(dt, Incident.ISO_FORMAT)
    return_datetime += timedelta(microseconds=int(microsecs))
    return return_datetime

  @staticmethod
  def GetDateTime(time):
    """Convert a datetime.datetime object to a Tasks API compatible string.

    Args:
      time: datetime.datetime to convert.
    Returns:
      String representing the datetime.datetime object.
    """
    date_str = time.isoformat()
    if len(date_str.split('.')) == 1:
      date_str += '.000'
    return date_str + 'Z'


class Message(db.Model):
  """Describes a message on from an incident.

  Attributes:
    message_id: RFC822 message ID. Populated when the message is an email.
    in_reply_to: RFC822 message ID that the message references most recently.
    references: Series of RFC822 message IDs, in reverse chronological order,
        that are referenced by the message.
    incident: Incident that the message belongs to.
    title: Title of the message.
    author: Author of the message.
    body: Body of the message in plaintext.
    sent: When the message was sent.
    mailing_list: Mailing list to which the message was sent (if the message
        is an email).
    canonical_link: Reference to the canonical location of the message, e.g. a
        the Google Group page or the Unify message.
  """
  message_id = db.StringProperty()
  in_reply_to = db.StringProperty()
  references = db.TextProperty()
  incident = db.ReferenceProperty(Incident)
  title = db.StringProperty(multiline=True)
  author = db.EmailProperty()
  body = db.TextProperty()
  sent = db.DateTimeProperty()
  mailing_list = db.StringProperty()
  canonical_link = db.StringProperty()

  def AssociateMailIncident(self):
    """Associates a message with an incident, using RFC822 message IDs.

    If the message refers to an existing incident, adds it to the
    incident. If the message does not refer to another message, or
    refers to a message not in the datastore, makes a new incident.

    Sets the 'incident.updated' field to utcnow.

    Sets the 'incident.training_review' to True since the new message
    can expand the current training set for the Prediction API.

    If the message is referenced by other incidents, merges those into
    the incident.
    """
    parent = Message.gql('WHERE message_id = :1', self.in_reply_to).get()

    if parent and parent.incident:
      logging.debug('Parent found: ' + parent.incident.title)
      self.incident = parent.incident
      self.put()

      parent.incident.PurgeJsonCache()
      parent.incident.updated = datetime.utcnow()
      parent.incident.training_review = True
      parent.incident.put()

      # Merge other incidents that point to this one, into this incident.
      children = Message.gql('WHERE in_reply_to = :1', self.message_id)
      Incident.MergeWithParent(parent.incident, children)
    else:
      children = Message.gql('WHERE in_reply_to = :1 ORDER BY sent ASC',
                             self.message_id)
      if children.count():
        logging.debug('Found child messages: ' + str(children.count()))

        # Update new message to refer to the oldest existing incident that
        # references it.
        incident = children[0].incident
        self.incident = incident
        self.put()

        # And update the incident with earlier metadata.
        incident.created = self.sent
        incident.updated = datetime.utcnow()
        incident.training_review = True
        incident.title = self.title
        incident.author = self.author
        incident.mailing_list = self.mailing_list
        incident.canonical_link = self.canonical_link
        incident.put()

        Incident.MergeWithParent(incident, children)
      else:
        logging.debug('New incident from: ' + self.message_id)
        # Or it must be a new incident...
        incident = Incident(title=self.title,
                            author=self.author,
                            created=self.sent,
                            mailing_list=self.mailing_list,
                            canonical_link=self.canonical_link)
        incident.put()

        self.incident = incident.key()
        self.put()

    logging.info('Adding to task queue incident_key=' +
                 str(self.incident.key()))
    taskqueue.add(queue_name='predictor', url='/tasks/suggest',
                  params={'incident_key': str(self.incident.key())})

  def ReferencesList(self):
    """Provides a list of RFC822 message IDs referenced by the message.

    Returns:
      List of RFC822 message IDs, in reverse chronological order, referenced by
      the message.
    """
    return self.references.split(',')

  @staticmethod
  def FromMail(mail, message_id, store_body=False):
    """Saves a mail message to the datastore.

    Args:
      mail: Incoming message to parse and save.
      message_id: Message-ID of the incoming message.
      store_body: Whether or not to store the message body.
    Returns:
      Saved message.
    """
    message = Message(message_id=message_id)
    message.canonical_link = Message.GetCanonicalLink(message_id)

    parsed_tz_tuple = email.utils.parsedate_tz(mail.date)
    time_tz = email.utils.mktime_tz(parsed_tz_tuple)
    message.sent = datetime.utcfromtimestamp(time_tz)
    if mail.original.get('Subject') and mail.subject:
      message.title = mail.subject

    m = re.search('.* <(.*)>', mail.sender)
    if m:
      message.author = m.group(1)
    else:
      message.author = mail.sender

    logging.debug('Received a message from: ' + message.author)

    if store_body:
      message.body = Message.GetMailBody(mail, 'text/plain')

    message.mailing_list = Message.GetMailingList(mail)

    references = mail.original.get_all('References')
    if references:
      message.references = ','.join(references)
      logging.debug(message.references)

    message.in_reply_to = Message.GetInReplyTo(mail, references)

    message.put()

    Message.RecordMailingList(message)
    Message._LogMessageIdDetails(message)

    return message

  @staticmethod
  def GetCanonicalLink(message_id):
    """Constructs the canonical link for an email.

    Args:
      message_id: Message-ID of the incoming message.
    Returns:
      Canonical link for the email.
    """
    base = 'https://mail.google.com/mail/#search/rfc822msgid%3A+'
    escaped = urllib.quote_plus(message_id)
    return base + escaped

  @staticmethod
  def GetMailBody(mail, body_type):
    """Retrieves the relevant mail body from the email.

    Args:
      mail: Incoming message to parse.
      body_type: Content type of the body to retrieve.
    Returns:
      Relevant mail body.
    """
    return list(mail.bodies(body_type))[0][1].decode()

  @staticmethod
  def GetInReplyTo(mail, references):
    """Retrieves the functional In-Reply-To header.

    If an actual In-Reply-To header is not found, one will be constructed by
    using the last entry of the References header, if it exists.

    Args:
      mail: Incoming message to parse.
      references: Mail references, from the References header.
    Returns:
      Functional In-Reply-To value.
    """
    in_reply_to = mail.original.get('In-Reply-To')

    if not in_reply_to and references:
      in_reply_to = references[-1].split('\n')[-1].split(' ')[-1]
      logging.debug('Using last reference instead of In-Reply-To')
      logging.debug(in_reply_to)

    if in_reply_to:
      single_line = in_reply_to.replace('\n', '')
      return single_line

    return None

  @staticmethod
  def GetMailingList(mail):
    """Retrieves the mailing list to which the message was sent.

    Will attempt to use one of two headers to find the mailing list.

    Args:
      mail: Incoming message to parse.
    Returns:
      Mailing list address.
    """
    if mail.original.get('Mailing-list'):
      m = re.search('list (.+);', mail.original.get('Mailing-list'))
      if m:
        return m.group(1)
    elif mail.original.get('List-Post'):
      m = re.search('<mailto:(.+)>', mail.original.get('List-Post'))
      if m:
        return m.group(1)

    return None

  @staticmethod
  def RecordMailingList(message):
    """Records existence of new mailing lists not previously recorded.

    If the incoming message does not have a mailing list, this is a no-op.

    Args:
      message: Datastore representation of the incoming message.
    """
    if message.mailing_list:
      logging.debug('Mailing-list: ' + message.mailing_list)
      list_entry = List.gql('WHERE email = :1', message.mailing_list).get()
      if not list_entry:
        logging.debug('List not found, adding entry')
        list_entry = List(email=message.mailing_list)
        list_entry.put()

  @staticmethod
  def _LogMessageIdDetails(message):
    """Saves debug information for the Message-ID and related fields.

    Args:
      message: Datastore representation of the incoming message.
    """
    if message.message_id:
      logging.debug('Message-ID: ' + message.message_id)

    if message.in_reply_to:
      logging.debug('In-Reply-To: ' + message.in_reply_to)

    if message.references:
      logging.debug('References: ' + message.references)

  def GetDict(self):
    """Return a dict representation of this message.

    This will return a copy from memcache if it exists (and caching is
    enabled), and update the cache if not present.

    Returns:
      Dict representing the incident.
    """
    key = self.GetJsonModelKey()
    cached = memcache.get(key)
    if cached and settings.USE_MEMCACHE_FOR_JSON_MODELS:
      return cached

    model = {
        'message_id': self.message_id,
        'in_reply_to': self.in_reply_to,
        'references': self.references,
        'title': self.title,
        'author': self.author,
        'body': self.body,
        'sent': self.sent,
        'mailing_list': self.mailing_list,
        'canonical_link': self.canonical_link}
    memcache.set(key, model)
    return model

  def GetJsonModelKey(self):
    """Returns the key pointing to the instance's JSON representation.

    Returns:
      JSON model key.
    """
    return settings.MEMCACHE_VERSION_PREFIX + str(self.key().id())


class List(db.Model):
  """Describes a mailing list.

  Attributes:
    name: Name of the mailing list.
    email: Email address of the mailing list.
  """
  name = db.StringProperty()
  email = db.EmailProperty()


class Tag(db.Model):
  """Describes a tag.

  A tag includes a model and a category. The model may be explicitly
  stated or, if it is missing, all tags with no explicit model are
  implicitly part of the same unspecified model.

  Attributes:

    name: Name of the tag and key of model object. Format:
      ["model""_MODEL_MARKER"]"category".  You can only set 'name'
      when you create the Tag because it is the key.
    example_count: Total count of current examples (with Accepted tags).
      trained_count: Count of examples at last training. At the moment
      of training, trained_count = example_count.
    trained_date: When this tag's examples were last sent to the Prediction API
  """
  example_count = db.IntegerProperty(default=0)
  trained_count = db.IntegerProperty(default=0)
  trained_date = db.DateTimeProperty()

  # _DEFAULT_MODEL is used when the user does not specify a model.
  # This app uses this string to name a training set for the Prediction
  # API, creating a file on Google Storage with this prefix. This
  # string also appears on the User Settings page to describe the
  # model created when the user does not specify a model. You might
  # want to change this string to localize it for presentation.

  _DEFAULT_MODEL = 'unspecified_model'

  # _DEFAULT_CATEGORY should never be seen or assigned since the UI
  # should always guarantee a non-blank Tag name. Provided as a safe
  # fallback. There is no need to change it.

  _DEFAULT_CATEGORY = 'unspecified_category'

  # _MODEL_MARKER defines the character which splits the model from
  # the category. If you change this then you must also change the
  # Javascript which enforces the tag definition in ui.js:
  # google.devrel.samples.autodo.Bindings.bindTagTextInput

  _MODEL_MARKER = '-'

  @property
  def name(self):
    """Get the Key name."""
    return self.key().name()

  @classmethod
  def ModelCategory(cls, tag):
    """Split a tag into a model and category.

    The goal is to isolate all the knowledge about how to parse a tag
    and model within Tag so that other functions don't have to change
    if we modify the format.

    Args:
      tag: String, the tag as typed by the user or Tag.name.

    Returns:
      Dictionary of [model, category, explicit]
      model: Group of competing tags.
      category: A classification within a model.
      explicit: True if model was specified,
                False if we applied default model name.
    """
    logging.info('TAG=%s', tag)
    split = dict(zip(('model', 'category'),
                     tag.split(cls._MODEL_MARKER)))

    if 'category' in split:
      split['explicit'] = True
    else:
      split['explicit'] = False
      split['category'] = split['model']
      split['model'] = cls._DEFAULT_MODEL

    if not split['model']:
      split['model'] = cls._DEFAULT_MODEL

    if not split['category']:
      split['category'] = cls._DEFAULT_CATEGORY

    return split

  @classmethod
  def ModelMatches(cls, model, tag):
    """Determine if a tag is a category of a model."""
    if cls._MODEL_MARKER not in tag and (
        not model or model == cls._DEFAULT_MODEL):
      return True
    else:
      return tag.startswith(model + cls._MODEL_MARKER)

  @classmethod
  def CreateMissingTags(cls, incident):
    """Create Tag Instances for tags in the given incident.

    Tags could have come from the Prediction API or the user.

    Args:
      incident: Incident to pull tags from for creation.
    """
    tags = set(incident.suggested_tags)
    tags.update(incident.accepted_tags)
    for tag in tags:
      # Use negative example_count to signal a new tag.
      tag_instance = cls.get_or_insert(tag, example_count=(-1))
      if tag_instance.example_count < 0:
        tag_instance.example_count = 0
        tag_instance.put()
        SuggestionModel.CreateMissingModel(tag)


class Credentials(db.Model):
  """Credentials Datastore class to store user's credentials information.

  Attributes:
    credentials: User's OAuth 2.0 credentials.
    email: User's email.
    user_id: User's ID (also used as key).
  """
  credentials = CredentialsProperty()
  email = db.StringProperty()

  @property
  def user_id(self):
    return self.key().name()


class UserSettings(db.Model):
  """Store user's settings.

  Attributes:
    tasks_credentials: Tasks API scoped credentials.
    email: User's email (also used as key).
    add_to_tasks: Whether or not to automatically add assigned incidents to
        the user's task list.
    task_list_id: ID of the task list to add the incidents to.
  """
  tasks_credentials = CredentialsProperty()
  add_to_tasks = db.BooleanProperty(default=False)
  task_list_id = db.StringProperty(default='@default')

  @property
  def email(self):
    return self.key().name()


class IncidentTask(db.Model):
  """Store link between an incident and a user's Task.

  Attributes:
    incident_id: ID of the incident (also used as key).
    task_id: ID of the user's task.
    task_list_id: ID of the user's task list.
    owner: Owner of this IncidentTask.
  """
  task_id = db.StringProperty()
  task_list_id = db.StringProperty(default='@default')
  owner = db.StringProperty()

  @property
  def incident_id(self):
    return self.key().name()


class SuggestionModel(db.Model):
  """Track data related to a model that was sent to the Prediction API.

  Attributes:
    name: The name of the model. Read-only. Set at creation time.
    training_file: Name of the Google Storage object for this model.
        Empty if never sent to Google Storage.
    training_date: Time and date when training was confirmed complete.
    training_tags: Tags included in the original training set.
    export_file: Name of downloadble file containing training set.
        Empty if never exported.
    export_date: Time and date when data last exported.
    export_tags: Tags included with exported data set.
    ui_tags: Tags to be shown in the UI as examples of this model.
        These tags could include new tags not yet added to a training or export.
  """
  training_file = db.StringProperty()
  training_date = db.DateTimeProperty()
  training_tags = db.ListProperty(str)
  training_examples = db.IntegerProperty(default=0)
  export_file = db.StringProperty()
  export_date = db.DateTimeProperty()
  export_tags = db.ListProperty(str)
  export_examples = db.IntegerProperty(default=0)
  ui_tags = db.ListProperty(str)

  @property
  def name(self):
    """Get the Key name."""
    return self.key().name()

  @classmethod
  def CreateMissingModel(cls, tag):
    """Create a new model for a tag, if necessary, and add tag to ui list.

    Args:
      tag: String name of a specific Tag.
    """
    model_name = Tag.ModelCategory(tag)['model']
    suggestion_model = cls.get_or_insert(model_name)
    suggestion_model.AddUITags([tag])
    suggestion_model.put()

  def AddUITags(self, tags):
    """Add one or more tags to this model for display in UI.

    This is strictly a convenience function for the UI and does not create a
    canonical list. The canonical lists are in training_tags and export_tags
    which contain all the tags present at the generation of those training sets.

    If the model name in the tag does not match this current model
    key_name then no change to the entity. If the model name matches
    then the tag will be added to the ui_tags set.

    Args:
      tags: list of Strings
    """
    ui_tags = [tag for tag in tags if Tag.ModelMatches(self.name, tag)]
    self.ui_tags = list(set(ui_tags))
