from kubernetes import client


def get_default_labels(name=None):
    default_labels = {
        'operator': 'memcached',
        'heritage': 'kubestack.com'}
    if name:
        default_labels['cluster'] = name
    return default_labels


def get_default_label_selector(name=None):
    default_labels = get_default_labels(name=name)
    default_label_selectors = []
    for label in default_labels:
        default_label_selectors.append(
            '{}={}'.format(label, default_labels[label]))
    return ','.join(default_label_selectors)


def get_service_object(name, namespace):
    service = client.V1Service()

    # Metadata
    service.metadata = client.V1ObjectMeta(
        name=name,
        namespace=namespace,
        labels=get_default_labels(name=name))
    # Add the monitoring label so that metrics get picked up by Prometheus
    service.metadata.labels['monitoring.kubestack.com'] = 'metrics'

    # Spec
    memcached_port = client.V1ServicePort(
        name='memcached', port=11211, protocol='TCP')
    metrics_port = client.V1ServicePort(
        name='metrics', port=9150, protocol='TCP')

    service.spec = client.V1ServiceSpec(
        cluster_ip='None',
        selector=get_default_labels(name=name),
        ports=[memcached_port, metrics_port])
    return service


def get_deployment_object(name, namespace, image, replicas):
    deployment = client.V1beta1Deployment()

    # Metadata
    deployment.metadata = client.V1ObjectMeta(
        name=name,
        namespace=namespace,
        labels=get_default_labels(name=name))

    # Spec
    deployment.spec = client.V1beta1DeploymentSpec(replicas=replicas)

    deployment.spec.template = client.V1PodTemplateSpec()
    deployment.spec.template.metadata = client.V1ObjectMeta(
        labels=get_default_labels(name=name))
    deployment.spec.template.spec = client.V1PodSpec()

    # Memcached container
    memcached_port = client.V1ContainerPort(
        name='memcached', container_port=11211, protocol='TCP')
    memcached_resources = client.V1ResourceRequirements(
        limits={'cpu': '100m', 'memory': '64Mi'},
        requests={'cpu': '100m', 'memory': '64Mi'})
    memcached_container = client.V1Container(
        name='memcached',
        command=['memcached', '-p', '11211'],
        image=image,
        ports=[memcached_port],
        resources=memcached_resources)

    # Metrics container
    metrics_port = client.V1ContainerPort(
        name='metrics', container_port=9150, protocol='TCP')
    metrics_resources = client.V1ResourceRequirements(
        limits={'cpu': '50m', 'memory': '16Mi'},
        requests={'cpu': '50m', 'memory': '16Mi'})
    metrics_container = client.V1Container(
        name='prometheus-exporter',
        image='prom/memcached-exporter:v0.3.0',
        ports=[metrics_port],
        resources=metrics_resources)

    deployment.spec.template.spec.containers = [
        memcached_container, metrics_container]
    return deployment
