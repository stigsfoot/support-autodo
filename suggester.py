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


"""Provides interface to the Google Prediction API.

This class provides suggested tags for Incidents and training sets for
the Prediction API.
"""



from datetime import datetime
import logging
import re
import string
import StringIO
import unicodedata
from apiclient.discovery import build
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
import gslite
import httplib2
import model
import settings


def CleanText(text, quote=False):
  """Transform a string into a string of terms suitable for a training set.

  The Prediction API treats each word as a separate term, so make all
  words lower case and remove all punctuation.  This is one area where
  experimenting with pre-processing can yield different prediction
  fidelity, so it is likely that this function should be updated for
  specialized parsing.

  This implementation converts everything to ASCII.

  Args:
    text: A string to be cleaned.
    quote: True if you want the results to be quoted.

  Returns:
    A string suitable for use in a .csv
  """
  to_remove = string.whitespace + string.punctuation

  text = unicodedata.normalize('NFKD', text).encode('ascii', 'replace')

  replace = re.compile('[%s]' % re.escape(to_remove))
  new_text = replace.sub(' ', text)
  new_text = re.sub(' +', ' ', new_text)
  new_text = new_text.lower()
  if quote:
    new_text = '\"%s\"' % new_text

  return new_text


def ConcatenateMessages(incident):
  """Find all the Messages attached to an Incident and return bodies.

  Args:
    incident: Incident instance which is the parent of the Messages.

  Returns:
    A string, possibly very long, containing all the text from all the
    Messages attached to an Incident.
  """
  messages = model.Message.gql('WHERE incident = :1', incident.key())
  return ' '.join([message.body for message in messages])


def RefreshTagsAndModels():
  """Update all the Tags and SuggestionModel.ui_tags."""
  incidents = model.Incident.all()
  for incident in incidents:
    model.Tag.CreateMissingTags(incident)

  tags = model.Tag.all()
  for tag in tags:
    model.SuggestionModel.CreateMissingModel(tag.name)


def BuildCSVRow(incident, tag=None, recycled=None):
  """Create an example row suitable for a training CSV file or streaming.

  This incident makes some expensive calls on text processing and
  data retrieval, so it returns all of the processed data in an
  "opaque" dictionary.  You can optionally pass this dictionary back
  in to the function through saveblob if you would like to save on
  processing.

  Args:
    incident: a model.Incident to parse.
    tag: String form of the tag name.  If present it will always be
        the first element of the returned string (per the Prediction
        API training format).
    recycled: "opaque" dictionary (for use only by this
        function). Modified by reference, you should pass this object
        back in if you are processing the same incident for multiple
        tags.

  Returns:
    String suitable for a row in the prediction (no tag) or
        training stream (with tag).
  """
  if recycled is None:
    recycled = {}
  if 'body' not in recycled:
    recycled['body'] = CleanText(ConcatenateMessages(incident))
    recycled['title'] = CleanText(incident.title)
  row_items = [recycled['title'], recycled['body']]
  if tag:
    # Tag should not be CleanText'd because it must match exactly.
    row_items.insert(0, tag)
  return ','.join(['\"%s\"' % item for item in row_items])


def BuildCSVTrainingSet(model_name, write_file, tag_counts, training=False):
  """Create a training set containing every example of a model.

  Args:
    model_name: String form of the model name.
    write_file: IO-based object with 'write' method.
    tag_counts: Dictionary of tags and a count of their examples.
    training: True if this CSV will be used immediately for training,
        to update the incident statistics

  Returns:
    Total number of examples in training set.
  """
  model_tags = []
  tags = model.Tag.all()

  for tag in tags:
    if model.Tag.ModelMatches(model_name, tag.name):
      model_tags.append(tag.name)

  incidents = model.Incident.all()
  # TODO(user) Add a filter to debounce incidents which have been
  # updated recently.  The user may still be making changes.

  # Note: "IN" queries are limited to 30 list elements (sub-queries)!
  if len(model_tags) > 30:
    logging.error('There are too many tags in %s to query with a single IN.',
                  model_name)
  incidents.filter('accepted_tags IN', model_tags)
  tag_counts_total = 0
  for incident in incidents:
    processed_incident = {}
    for tag in incident.accepted_tags:
      if tag in model_tags:
        if training:
          incident.trained_tags.append(tag)
        write_file.write(BuildCSVRow(incident, tag=tag,
                                     recycled=processed_incident))
        write_file.write('\n')
        tag_counts_total += 1
        if tag in tag_counts:
          tag_counts[tag] += 1
        else:
          tag_counts[tag] = 1
    if training:
      incident.trained_tags = list(set(incident.trained_tags))
      incident.updated = datetime.utcnow()
      incident.trained_date = incident.updated
      # incident.training_review should remain unchanged because we
      # only checked one model.  This incident may belong to multiple
      # models, some of which have already been trained.
      incident.put()

  return tag_counts_total


class Suggester(webapp.RequestHandler):
  """Learn and suggest tags for Incidents.

  Learn from user-provided tags ("Accepted" tags)
  and suggest tags for Incidents as Messages arrive.
  """

  def _SuggestTags(self, key, service):
    """Get suggestions for tags from the Prediction API.

    Args:
      key: Model Key for Incident to receive suggested tags.
      service: Built API service class, pre-authorized for OAuth.

    Returns:
      Nothing.  Updates Incident in Datastore.
    """
    incident = db.get(key)
    if incident is None:
      logging.error('_SuggestTags: No Incident with id=' + key)
    else:
      csv_instance = BuildCSVRow(incident)
      sample = {'input': {'csvInstance': [csv_instance]}}
      bucket = settings.GS_BUCKET
      model_list = model.SuggestionModel.all()
      suggested = []

      for suggestion_model in model_list:
        if suggestion_model.training_examples:
          full_name = '%s/%s' % (bucket, suggestion_model.training_file)
          prediction = service.predict(body=sample, data=full_name).execute()
          logging.info('Model:%s Prediction=%s', suggestion_model.name,
                       prediction)
          suggested.append(prediction['outputLabel'])

      if suggested:
        incident.suggested_tags = suggested
        logging.info('_SuggestTags: Final Suggestions=%s', ','.join(suggested))
        incident.PurgeJsonCache()
        incident.updated = datetime.utcnow()
        incident.put()
        model.Tag.CreateMissingTags(incident)

  def post(self):
    """Handle a POST request by returning suggestions from the prediction API.

    POST Parameters:
      incident_key: String form of Incident Key.

    Returns:
      Nothing.  Modifies Incident.suggested_tags.
    """
    logging.info('Suggester.post')
    incident_key = self.request.get('incident_key')
    if incident_key is None:
      logging.error('No incident_key provided')
      return
    else:
      incident_key = db.Key(incident_key)
      credentials = model.Credentials.get_by_key_name(
          settings.CREDENTIALS_KEYNAME)
      if credentials is not None:
        credentials = credentials.credentials
        http = httplib2.Http()
        http = credentials.authorize(http)
        service = build('prediction', 'v1.2', http=http)
        self._SuggestTags(incident_key, service)


class Trainer(webapp.RequestHandler):
  """Make Examples and train the Prediction Engine from the Examples."""

  def _UpdateTraining(self, training):
    """Update the Prediction API training model with new models and examples.

    Args:
      training: The Prediction API training service, already authorized.
    """
    trained_model_query = db.GqlQuery('SELECT * FROM SuggestionModel '
                                      'WHERE training_examples > 0')
    trained_model_names = {}
    for trained_model in trained_model_query:
      trained_model_names[trained_model.name] = trained_model.training_file
      logging.info('TRAINED MODEL=%s', trained_model.name)

    # Note on Query design: I originally wanted to select where
    # updated>trained, but the right value (trained) cannot be another
    # column in the Incident, it must be a constant.  Instead I
    # created a new field, training_review, which is True when
    # training should look at the Incident for changes and False when
    # the Incident has been processed.
    # TODO(user): optimize training_review so that it is only set
    # when tags change.  Right now it is set whenever the Incident is
    # updated.
    updated_incidents = db.GqlQuery('SELECT * FROM Incident '
                                    'WHERE training_review = TRUE')
    for updated_incident in updated_incidents:
      logging.info('UPDATED INCIDENT = ' + updated_incident.title)
      processed_incident = {}
      new_tags = (set(updated_incident.accepted_tags) -
                  set(updated_incident.trained_tags))

      for new_tag in new_tags:
        new_tag_model = model.Tag.ModelCategory(new_tag)['model']
        if new_tag_model in trained_model_names:
          example = BuildCSVRow(updated_incident, tag=new_tag,
                                recycled=processed_incident)
          logging.info('%s\n\tROW = %s', trained_model_names[new_tag_model],
                       example)
          current_model = model.SuggestionModel.get_by_key_name(new_tag_model)
          gs_full_name = '%s/%s' % (settings.GS_BUCKET,
                                    current_model.training_file)
          csv_instance = {'classLabel': new_tag, 'csvInstance': [example]}
          # TODO(user) Check training result for success.
          training.update(data=gs_full_name,
                          body=csv_instance).execute()
          updated_incident.trained_tags.append(new_tag)

      updated_incident.trained_tags = list(set(updated_incident.trained_tags))
      updated_incident.training_review = False
      updated_incident.put()

    # Go through the untrained models second because they can ignore the
    # training_review flag.
    untrained_models = db.GqlQuery('SELECT * FROM SuggestionModel '
                                   'WHERE training_examples = 0')

    storage = gslite.GsClient(access_key=settings.GS_LEGACY_ACCESS,
                              secret=settings.GS_LEGACY_SECRET)
    tag_counts = {}
    for untrained_model in untrained_models:
      logging.info('UNTRAINED MODEL = ' + untrained_model.name)
      string_file = StringIO.StringIO()
      example_count_total = BuildCSVTrainingSet(untrained_model.name,
                                                string_file, tag_counts,
                                                training=True)
      if example_count_total:
        gs_object_name = 'testing/' + untrained_model.name
        gs_full_name = '%s/%s' % (settings.GS_BUCKET, gs_object_name)
        body = {'id': gs_full_name}

        storage.put_object(settings.GS_BUCKET, gs_object_name, string_file)
        string_file.close()
        # TODO(user) check result for success
        training.insert(body=body).execute()
        untrained_model.training_file = gs_object_name
        untrained_model.training_date = datetime.utcnow()
        untrained_model.training_examples = example_count_total
        untrained_model.training_tags = tag_counts.keys()
        untrained_model.put()

    # Update the statistics in the related Tag
    for tag in tag_counts:
      tag_object = model.Tag.get_by_key_name(tag)
      tag_object.example_count = tag_counts[tag]
      tag_object.trained_count = tag_counts[tag]
      tag_object.trained_date = datetime.utcnow()
      tag_object.put()

  def _DownloadCSV(self, model_name):
    """Generate a csv file suitable for use as a training set.

    Provides download file and updates Tags in datastore.

    Args:
      model_name: model.name.  All Accepted tags for this model will be
        processed to create one training set.
    """
    now = datetime.utcnow()
    suggestion_model = model.SuggestionModel.get_by_key_name(model_name)
    suggestion_model.export_file = '%s-%s.csv' % (model_name, now.isoformat())
    disposition = 'attachment; filename=%s' % suggestion_model.export_file

    self.response.headers['Content-Type'] = 'text/csv'
    self.response.headers['Content-Disposition'] = disposition

    tag_counts = {}
    temp_file = StringIO.StringIO()
    example_count_total = BuildCSVTrainingSet(model_name, temp_file,
                                              tag_counts)
    self.response.out.write(temp_file.getvalue())
    temp_file.close()

    # Update the statistics in the related Tag
    for tag in tag_counts:
      tag_object = model.Tag.get_by_key_name(tag)
      tag_object.example_count = tag_counts[tag]
      tag_object.put()

    # Update the statistics in the SuggestionModel
    suggestion_model.export_date = now
    suggestion_model.export_tags = tag_counts.keys()
    suggestion_model.ui_tags = suggestion_model.export_tags
    suggestion_model.export_examples = example_count_total
    suggestion_model.put()

  def post(self):
    """Process requests to train or for training data.

    Possible requests:
    action=train: force a new training set for all tags with sufficient
    Examples.
    action=status: report on the training status for all tags.
    action=create: generate a set of terms from an Incident for each tag.
    """
    action = self.request.get('action')
    model_name = self.request.get('model_name')

    if action == 'csv':
      self._DownloadCSV(model_name)
    elif action == 'refresh':
      RefreshTagsAndModels()
      credentials = model.Credentials.get_by_key_name(
          settings.CREDENTIALS_KEYNAME)
      if credentials is not None:
        credentials = credentials.credentials
        http = httplib2.Http()
        http = credentials.authorize(http)
        service = build('prediction', 'v1.2', http=http)
        train = service.training()
        self._UpdateTraining(train)
      self.redirect('/')


def main():
  run_wsgi_app(webapp.WSGIApplication([
      ('/tasks/train', Trainer),
      ('/tasks/suggest', Suggester)]))

if __name__ == '__main__':
  main()
