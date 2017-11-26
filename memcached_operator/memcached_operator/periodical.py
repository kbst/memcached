import logging
from time import sleep

from kubernetes import client

from .kubernetes_resources import (get_default_label_selector,
                                   get_mcrouter_service_object,
                                   get_memcached_service_object)
from .kubernetes_helpers import (list_cluster_memcached_object,
                                 get_namespaced_memcached_object,
                                 create_service,
                                 update_service,
                                 delete_service,
                                 create_memcached_deployment,
                                 create_mcrouter_deployment,
                                 update_memcached_deployment,
                                 update_mcrouter_deployment,
                                 delete_deployment)


def periodical_check(shutting_down, sleep_seconds):
    logging.info('thread started')
    while not shutting_down.isSet():
        try:
            # First make sure all expected resources exist
            check_existing()

            # Then garbage collect resources from deleted clusters
            collect_garbage()
        except Exception as e:
            # Last resort: catch all exceptions to keep the thread alive
            logging.exception(e)
        finally:
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
    try:
        cluster_list = list_cluster_memcached_object()
    except client.rest.ApiException as e:
        # If for any reason, k8s api gives us an error here, there is
        # nothing for us to do but retry later
        logging.exception(e)
        return False

    core_api = client.CoreV1Api()
    apps_api = client.AppsV1beta1Api()
    for cluster_object in cluster_list['items']:
        name = cluster_object['metadata']['name']
        namespace = cluster_object['metadata']['namespace']

        service_objects = [
            get_mcrouter_service_object(cluster_object),
            get_memcached_service_object(cluster_object)]
        for service_object in service_objects:
            # Check service exists
            service_name = service_object.metadata.name
            try:
                service = core_api.read_namespaced_service(service_name, namespace)
            except client.rest.ApiException as e:
                if e.status == 404:
                    # Create missing service
                    created_service = create_service(service_object)
                    if created_service:
                        # Store latest version in cache
                        cache_version(created_service)
                else:
                    logging.exception(e)
            else:
                if not is_version_cached(service):
                    # Update since we don't know if it's configured correctly
                    updated_service = update_service(service_object)
                    if updated_service:
                        # Store latest version in cache
                        cache_version(updated_service)

        # Check memcached deployment exists
        try:
            deployment = apps_api.read_namespaced_deployment(name, namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # Create missing deployment
                created_memcached_deployment = create_memcached_deployment(
                    cluster_object)
                if created_memcached_deployment:
                    # Store latest version in cache
                    cache_version(created_memcached_deployment)
            else:
                logging.exception(e)
        else:
            if not is_version_cached(deployment):
                # Update since we don't know if it's configured correctly
                updated_memcached_deployment = update_memcached_deployment(cluster_object)
                if updated_memcached_deployment:
                    # Store latest version in cache
                    cache_version(updated_memcached_deployment)

        # Check mcrouter deployment exists
        try:
            deployment = apps_api.read_namespaced_deployment(
                '{}-router'.format(name),
                namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # Create missing deployment
                created_mcrouter_deployment = create_mcrouter_deployment(
                    cluster_object)
                if created_mcrouter_deployment:
                    # Store latest version in cache
                    cache_version(created_mcrouter_deployment)
            else:
                logging.exception(e)
        else:
            if not is_version_cached(deployment):
                # Update since we don't know if it's configured correctly
                updated_mcrouter_deployment = update_mcrouter_deployment(cluster_object)
                if updated_mcrouter_deployment:
                    # Store latest version in cache
                    cache_version(updated_mcrouter_deployment)


def collect_garbage():
    core_api = client.CoreV1Api()
    apps_api = client.AppsV1beta1Api()
    label_selector = get_default_label_selector()

    # Find all services that match our labels
    try:
        service_list = core_api.list_service_for_all_namespaces(
            label_selector=label_selector)
    except client.rest.ApiException as e:
        logging.exception(e)
    else:
        # Check if service belongs to an existing cluster
        for service in service_list.items:
            cluster_name = service.metadata.labels['cluster']
            name = service.metadata.name
            namespace = service.metadata.namespace

            try:
                get_namespaced_memcached_object(
                    cluster_name, namespace)
            except client.rest.ApiException as e:
                if e.status == 404:
                    # Delete service
                    delete_service(name, namespace)
                else:
                    logging.exception(e)

    # Find all deployments that match our labels
    try:
        deployment_list = apps_api.list_deployment_for_all_namespaces(
            label_selector=label_selector)
    except client.rest.ApiException as e:
        logging.exception(e)
    else:
        # Check if deployment belongs to an existing cluster
        for deployment in deployment_list.items:
            cluster_name = deployment.metadata.labels['cluster']
            name = deployment.metadata.name
            namespace = deployment.metadata.namespace

            try:
                get_namespaced_memcached_object(
                    cluster_name, namespace)
            except client.rest.ApiException as e:
                if e.status == 404:
                    # Delete deployment, replicasets and pods
                    delete_deployment(name, namespace)
                else:
                    logging.exception(e)
