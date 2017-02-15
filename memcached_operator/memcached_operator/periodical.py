import logging
from time import sleep

from kubernetes import client

from .memcached_tpr_v1alpha1_api import MemcachedThirdPartyResourceV1Alpha1Api
from .kubernetes_resources import get_default_label_selector
from .kubernetes_helpers import (create_service, update_service,
                                 delete_service, create_deployment,
                                 update_deployment, reap_deployment)


def periodical_check(shutting_down, sleep_seconds):
    logging.info('thread started')
    while not shutting_down.isSet():
        # First make sure all expected resources exist
        check_existing()

        # Then garbage collect resources from deleted clusters
        collect_garbage()

        sleep(int(sleep_seconds))
    else:
        logging.info('thread stopped')


VERSION_CACHE = {}


def is_version_cached(resource):
    uid = resource.metadata.uid
    version = resource.metadata.resource_version

    if uid in VERSION_CACHE and VERSION_CACHE[uid] == version:
        return True

    return False


def cache_version(resource):
    uid = resource.metadata.uid
    version = resource.metadata.resource_version

    VERSION_CACHE[uid] = version


def check_existing():
    memcached_tpr_api = MemcachedThirdPartyResourceV1Alpha1Api()
    try:
        cluster_list = memcached_tpr_api.list_memcached_for_all_namespaces()
    except client.rest.ApiException as e:
        logging.exception(e)

    v1 = client.CoreV1Api()
    v1beta1api = client.ExtensionsV1beta1Api()
    for cluster_object in cluster_list['items']:
        name = cluster_object['metadata']['name']
        namespace = cluster_object['metadata']['namespace']

        # Check service exists
        try:
            service = v1.read_namespaced_service(name, namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # Create missing service
                created_service = create_service(cluster_object)
                # Store latest version in cache
                cache_version(created_service)
            else:
                logging.exception(e)
        else:
            if not is_version_cached(service):
                # Update since we don't know if it's configured correctly
                updated_service = update_service(cluster_object)
                # Store latest version in cache
                cache_version(updated_service)

        # Check deployment exists
        try:
            deployment = v1beta1api.read_namespaced_deployment(name, namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # Create missing deployment
                created_deployment = create_deployment(cluster_object)
                # Store latest version in cache
                cache_version(created_deployment)
            else:
                logging.exception(e)
        else:
            if not is_version_cached(deployment):
                # Update since we don't know if it's configured correctly
                updated_deployment = update_deployment(cluster_object)
                # Store latest version in cache
                cache_version(updated_deployment)


def collect_garbage():
    # Find all services that match our labels
    memcached_tpr_api = MemcachedThirdPartyResourceV1Alpha1Api()
    v1 = client.CoreV1Api()
    label_selector = get_default_label_selector()
    try:
        service_list = v1.list_service_for_all_namespaces(
            label_selector=label_selector)
    except client.rest.ApiException as e:
        logging.exception(e)

    # Check if service belongs to an existing cluster
    for service in service_list.items:
        name = service.metadata.name
        namespace = service.metadata.namespace

        try:
            memcached_tpr_api.read_namespaced_memcached(name, namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # Delete service
                delete_service(name, namespace)
            else:
                logging.exception(e)

    # Find all deployments that match our labels
    v1beta1api = client.ExtensionsV1beta1Api()
    label_selector = get_default_label_selector()
    try:
        deployment_list = v1beta1api.list_deployment_for_all_namespaces(
            label_selector=label_selector)
    except client.rest.ApiException as e:
        logging.exception(e)

    # Check if deployment belongs to an existing cluster
    for deployment in deployment_list.items:
        name = deployment.metadata.name
        namespace = deployment.metadata.namespace

        try:
            memcached_tpr_api.read_namespaced_memcached(name, namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # Gracefully delete deployment, replicaset and pods
                reap_deployment(name, namespace)
            else:
                logging.exception(e)
