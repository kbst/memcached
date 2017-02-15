import logging

from kubernetes import watch

from .memcached_tpr_v1alpha1_api import MemcachedThirdPartyResourceV1Alpha1Api
from .kubernetes_helpers import (create_service, delete_service,
                                 create_deployment, reap_deployment)


def event_listener(shutting_down, timeout_seconds):
    logging.info('thread started')
    memcached_tpr_api = MemcachedThirdPartyResourceV1Alpha1Api()
    event_watch = watch.Watch()
    while not shutting_down.isSet():
        for event in event_watch.stream(
                memcached_tpr_api.list_memcached_for_all_namespaces,
                timeout_seconds=timeout_seconds):

            event_switch(event)
    else:
        event_watch.stop()
        logging.info('thread stopped')


def event_switch(event):
    if not 'type' in event and not 'object' in event:
        # We can't work with that event
        logging.warning('malformed event: {}'.format(event))
        return

    event_type = event['type']
    cluster_object = event['object']

    if event_type == 'ADDED':
        add(cluster_object)
    elif event_type == 'MODIFIED':
        modify(cluster_object)
    elif event_type == 'DELETED':
        delete(cluster_object)

def add(cluster_object):
    # Create service
    create_service(cluster_object)

    # Create deployment
    create_deployment(cluster_object)


def modify(cluster_object):
    logging.warning('UPDATE NOT IMPLEMENTED YET')


def delete(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    # Delete service
    delete_service(name, namespace)

    # Gracefully delete deployment, replicaset and pods
    reap_deployment(name, namespace)
