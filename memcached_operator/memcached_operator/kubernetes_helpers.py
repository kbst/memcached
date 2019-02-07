import logging
from time import sleep

from kubernetes import client

from .kubernetes_resources import (get_default_label_selector,
                                   get_memcached_deployment_object,
                                   get_mcrouter_deployment_object)


def list_cluster_memcached_object(**kwargs):
    custom_object_api = client.CustomObjectsApi()
    cluster_list = custom_object_api.list_cluster_custom_object(
        'kubestack.com',
        'v1',
        'memcacheds',
        **kwargs)
    return cluster_list


def get_namespaced_memcached_object(name, namespace):
    custom_object_api = client.CustomObjectsApi()
    cluster = custom_object_api.get_namespaced_custom_object(
        'kubestack.com',
        'v1',
        namespace,
        'memcacheds',
        name)
    return cluster

def create_service(service_object):
    name = service_object.metadata.name
    namespace = service_object.metadata.namespace
    v1 = client.CoreV1Api()
    try:
        service = v1.create_namespaced_service(namespace, service_object)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Service already exists
            logging.debug('svc/{} in ns/{} already exists'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created svc/{} in ns/{}'.format(name, namespace))
        return service


def update_service(service_object):
    name = service_object.metadata.name
    namespace = service_object.metadata.namespace
    v1 = client.CoreV1Api()
    try:
        service = v1.patch_namespaced_service(name, namespace, service_object)
    except client.rest.ApiException as e:
        logging.exception(e)
        return False
    else:
        logging.info('updated svc/{} in ns/{}'.format(name, namespace))
        return service


def delete_service(name, namespace, delete_options=None):
    v1 = client.CoreV1Api()
    if not delete_options:
        delete_options = client.V1DeleteOptions()
    try:
        v1.delete_namespaced_service(name, namespace, delete_options)
    except client.rest.ApiException as e:
        logging.exception(e)
        return False
    else:
        logging.info('deleted svc/{} from ns/{}'.format(name, namespace))
        return True


def create_memcached_deployment(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    apps_api = client.AppsV1beta1Api()
    body = get_memcached_deployment_object(cluster_object)
    try:
        deployment = apps_api.create_namespaced_deployment(namespace, body)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Deployment already exists
            logging.debug('deploy/{} in ns/{} already exists'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created deploy/{} in ns/{}'.format(name, namespace))
        return deployment


def create_mcrouter_deployment(cluster_object):
    name = '{}-router'.format(cluster_object['metadata']['name'])
    namespace = cluster_object['metadata']['namespace']
    apps_api = client.AppsV1beta1Api()
    body = get_mcrouter_deployment_object(cluster_object)
    try:
        deployment = apps_api.create_namespaced_deployment(namespace, body)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Deployment already exists
            logging.debug('deploy/{} in ns/{} already exists'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created deploy/{} in ns/{}'.format(name, namespace))
        return deployment


def update_memcached_deployment(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    apps_api = client.AppsV1beta1Api()
    body = get_memcached_deployment_object(cluster_object)
    try:
        deployment = apps_api.patch_namespaced_deployment(
            name, namespace, body)
    except client.rest.ApiException as e:
        logging.exception(e)
        return False
    else:
        logging.info('updated deploy/{} in ns/{}'.format(name, namespace))
        return deployment


def update_mcrouter_deployment(cluster_object):
    name = '{}-router'.format(cluster_object['metadata']['name'])
    namespace = cluster_object['metadata']['namespace']
    apps_api = client.AppsV1beta1Api()
    body = get_mcrouter_deployment_object(cluster_object)
    try:
        deployment = apps_api.patch_namespaced_deployment(
            name, namespace, body)
    except client.rest.ApiException as e:
        logging.exception(e)
        return False
    else:
        logging.info('updated deploy/{} in ns/{}'.format(name, namespace))
        return deployment


def delete_deployment(name, namespace, delete_options=None):
    apps_api = client.AppsV1beta1Api()
    if not delete_options:
        delete_options = client.V1DeleteOptions(
            propagation_policy='Background')
    try:
        apps_api.delete_namespaced_deployment(
            name, namespace, delete_options)
    except client.rest.ApiException as e:
        if e.status == 404:
            # Deployment does not exist, nothing to delete but
            # we can consider this a success.
            logging.debug(
                'not deleting nonexistent deploy/{} from ns/{}'.format(
                    name, namespace))
            return True
        else:
            logging.exception(e)
            return False
    else:
        logging.info('deleted deploy/{} from ns/{}'.format(
            name, namespace))
        return True
