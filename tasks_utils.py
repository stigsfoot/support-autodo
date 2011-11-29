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


"""Provides utility functions for Tasks API."""



import logging
import traceback
from apiclient import errors
from apiclient.discovery import build
from oauth2client.appengine import StorageByKeyName
import httplib2
import model


def _BuildClient(credentials):
  """Build a Tasks client.

  Args:
    credentials: Credentials used to authorized requests.
  Returns:
    Tasks API client.
  """
  http = httplib2.Http()
  if credentials:
    http = credentials.authorize(http)
  return build('tasks', 'v1', http=http)


def _GetCredentialsAndSettings(user_email):
  """Retrieve the user's credentials and settings.

  Args:
    user_email: Email of the user to retrieve settings and credentials for.
  Returns:
    User's credentials and settings as a tuple.
  """
  settings = model.UserSettings.get_by_key_name(user_email)
  credentials = None
  if settings:
    credentials = StorageByKeyName(
        model.UserSettings, user_email, 'tasks_credentials').get()
  return credentials, settings


def AddTask(incident, client=None):
  """Retrieve the owner's settings and add a task if requested.

  Args:
    incident: Incident to add the task for.
    client: TasksClient to use for Tasks API requests.
  """
  if incident.owner and incident.owner != 'none':
    credentials, settings = _GetCredentialsAndSettings(incident.owner)
    if credentials and settings.add_to_tasks:
      client = client or _BuildClient(credentials)
      key_name = '%s' % incident.key().id()
      incident_task = model.IncidentTask.get_by_key_name(key_name)
      body = incident.ToTaskDict()
      try:
        task = client.tasks().insert(
            tasklist=settings.task_list_id, body=body).execute()
      except errors.HttpError:
        logging.error(
            'Exception occured inserting task for incident %s',
            incident.key().id())
        logging.error(traceback.format_exc()[:-1])
        if incident_task:
          # Task could not be added, remove reference.
          incident_task.delete()
      else:
        if not incident_task:
          incident_task = model.IncidentTask(key_name=key_name)
        incident_task.task_id = task['id']
        incident_task.task_list_id = settings.task_list_id
        incident_task.owner = incident.owner
        incident_task.put()


def RemoveTask(incident, client=None):
  """Retrieve the owner's settings and delete the incdent's task if existing.

  Args:
    incident: Incident to remove the task for.
    client: TasksClient to use for Tasks API requests.
  """
  incident_task = model.IncidentTask.get_by_key_name(
      '%s' % incident.key().id())
  if incident_task:
    credentials, settings = _GetCredentialsAndSettings(incident_task.owner)
    if credentials and settings.add_to_tasks:
      client = client or _BuildClient(credentials)
      try:
        client.tasks().delete(
            tasklist=incident_task.task_list_id,
            task=incident_task.task_id).execute()
      except errors.HttpError:
        logging.error(
            'Exception occured while deleting task %s - %s',
            incident_task.task_list_id, incident_task.task_id)
        logging.error(traceback.format_exc()[:-1])
    else:
      logging.warning(
          'No owner or credentials found for IncidentTask %s',
          incident.key().id())
    incident_task.delete()
  else:
    logging.warning(
        'No IncidentTask found for incident %s', incident.key().id())


def UpdateTask(incident, old_client=None, new_client=None):
  """Update Task information on an updated incident.

  Args:
    incident: New version of the incident.
    old_client: TasksClient to use for Tasks API requests.
    new_client: TasksClient to use for Tasks API requests.
  """
  incident_task = model.IncidentTask.get_by_key_name(
      '%s' % incident.key().id())
  if not incident_task:
    AddTask(incident, new_client)
  else:
    old_credentials = _GetCredentialsAndSettings(incident_task.owner)[0]
    old_client = old_client or _BuildClient(old_credentials)
    if incident_task.owner == incident.owner:
      if old_credentials:
        try:
          old_task = old_client.tasks().get(
              tasklist=incident_task.task_list_id,
              task=incident_task.task_id).execute()
          old_task = incident.ToTaskDict(old_task)
          old_client.tasks().update(
              tasklist=incident_task.task_list_id, task=incident_task.task_id,
              body=old_task).execute()
        except errors.HttpError:
          logging.error(
              'Exception occured while retrieving or updating task %s - %s',
              incident_task.task_list_id, incident_task.task_id)
          logging.error(traceback.format_exc()[:-1])
      else:
        logging.warning(
            'No credentials found for IncidentTask #%s',
            incident_task.key().id())
        incident_task.delete()
    else:
      # If the owner changed, delete the task for the previous owner.
      if old_credentials:
        try:
          old_client.tasks().delete(
              tasklist=incident_task.task_list_id,
              task=incident_task.task_id).execute()
        except errors.HttpError:
          logging.error(
              'Exception occured while deleting task %s - %s',
              incident_task.task_list_id, incident_task.task_id)
          logging.error(traceback.format_exc()[:-1])
      else:
        logging.warning(
            'No credentials found for IncidentTask #%s',
            incident_task.key().id())
      new_credentials, new_settings = _GetCredentialsAndSettings(
          incident.owner)
      if new_credentials and new_settings.add_to_tasks:
        AddTask(incident, new_client)
      else:
        incident_task.delete()
