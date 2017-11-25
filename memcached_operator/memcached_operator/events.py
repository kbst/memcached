import logging
from time import sleep

from kubernetes import watch

from .kubernetes_helpers import (list_cluster_memcached_object,
                                 create_service,
                                 update_service,
                                 delete_service,
                                 create_memcached_deployment,
                                 update_memcached_deployment,
                                 create_mcrouter_deployment,
                                 update_mcrouter_deployment,
                                 reap_deployment)
from .kubernetes_resources import (get_mcrouter_service_object,
                                   get_memcached_service_object)


def event_listener(shutting_down, timeout_seconds):
    logging.info('thread started')
    event_watch = watch.Watch()
    while not shutting_down.isSet():
        try:
            for event in event_watch.stream(
                    list_cluster_memcached_object,
                    _request_timeout=timeout_seconds):

                event_switch(event)
        except Exception as e:
            # Last resort: catch all exceptions to keep the thread alive
            logging.exception(e)
            sleep(int(timeout_seconds))
    else:
        event_watch.stop()
        logging.info('thread stopped')


def event_switch(event):
    if 'type' not in event and 'object' not in event:
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
    # Create services
    create_service(get_mcrouter_service_object(cluster_object))
    create_service(get_memcached_service_object(cluster_object))

    # Create deployments
    create_memcached_deployment(cluster_object)
    create_mcrouter_deployment(cluster_object)


def modify(cluster_object):
    # Update services
    update_service(get_mcrouter_service_object(cluster_object))
    update_service(get_memcached_service_object(cluster_object))

    # Update deployments
    update_memcached_deployment(cluster_object)
    update_mcrouter_deployment(cluster_object)


def delete(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    # Delete service
    delete_service(name, namespace)
    delete_service('{}-backend'.format(name), namespace)

    # Gracefully delete deployment, replicaset and pods
    reap_deployment(name, namespace)
