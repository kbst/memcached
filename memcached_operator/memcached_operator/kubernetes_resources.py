import json

from kubernetes import client


def get_default_labels(name=None):
    default_labels = {
        'operated-by': 'memcached.operator.kubestack.com',
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


def get_mcrouter_service_object(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
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
        selector=get_default_labels(name=name),
        ports=[memcached_port, metrics_port])
    service.spec.selector['service-type'] = 'mcrouter'
    return service


def get_memcached_service_object(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    service = client.V1Service()

    # Metadata
    service.metadata = client.V1ObjectMeta(
        name='{}-backend'.format(name),
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
    service.spec.selector['service-type'] = 'memcached'
    return service


def get_memcached_deployment_object(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']

    try:
        replicas = cluster_object['spec']['memcached']['replicas']
    except KeyError:
        replicas = 2

    try:
        memcached_limit_cpu = \
            cluster_object['spec']['memcached']['memcached_limit_cpu']
    except KeyError:
        memcached_limit_cpu = '100m'

    try:
        memcached_limit_memory = \
            cluster_object['spec']['memcached']['memcached_limit_memory']
    except KeyError:
        memcached_limit_memory = '64Mi'

    deployment = client.AppsV1beta1Deployment()

    # Metadata
    deployment.metadata = client.V1ObjectMeta(
        name=name,
        namespace=namespace,
        labels=get_default_labels(name=name))
    deployment.metadata.labels['service-type'] = 'memcached'

    # Spec
    deployment.spec = client.AppsV1beta1DeploymentSpec(
        replicas=replicas,
        template=client.V1PodTemplateSpec())

    deployment.spec.template.metadata = client.V1ObjectMeta(
        labels=deployment.metadata.labels)
    deployment.spec.template.spec = client.V1PodSpec(containers=[])

    # Memcached container
    memcached_port = client.V1ContainerPort(
        name='memcached', container_port=11211, protocol='TCP')
    memcached_resources = client.V1ResourceRequirements(
        limits={
            'cpu': memcached_limit_cpu, 'memory': memcached_limit_memory},
        requests={
            'cpu': memcached_limit_cpu, 'memory': memcached_limit_memory})
    memcached_container = client.V1Container(
        name='memcached',
        command=['memcached', '-p', '11211'],
        image='memcached:1.4.33',
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


def get_mcrouter_deployment_object(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']

    try:
        replicas = cluster_object['spec']['mcrouter']['replicas']
    except KeyError:
        replicas = 1

    try:
        mcrouter_limit_cpu = \
            cluster_object['spec']['mcrouter']['mcrouter_limit_cpu']
    except KeyError:
        mcrouter_limit_cpu = '50m'

    try:
        mcrouter_limit_memory = \
            cluster_object['spec']['mcrouter']['mcrouter_limit_memory']
    except KeyError:
        mcrouter_limit_memory = '32Mi'

    deployment = client.AppsV1beta1Deployment()

    # Metadata
    deployment.metadata = client.V1ObjectMeta(
        name="{}-router".format(name),
        namespace=namespace,
        labels=get_default_labels(name=name))
    deployment.metadata.labels['service-type'] = 'mcrouter'

    # Spec
    deployment.spec = client.AppsV1beta1DeploymentSpec(
        replicas=replicas,
        template=client.V1PodTemplateSpec())

    deployment.spec.template = client.V1PodTemplateSpec()
    deployment.spec.template.metadata = client.V1ObjectMeta(
        labels=deployment.metadata.labels)
    deployment.spec.template.spec = client.V1PodSpec(containers=[])

    # Mcrouter container
    mcrouter_config_volumemount = client.V1VolumeMount(
        name='mcrouter-config',
        read_only=False,
        mount_path='/etc/mcrouter')

    mcrouter_port = client.V1ContainerPort(
        name='mcrouter', container_port=11211, protocol='TCP')
    mcrouter_resources = client.V1ResourceRequirements(
        limits={
            'cpu': mcrouter_limit_cpu, 'memory': mcrouter_limit_memory},
        requests={
            'cpu': mcrouter_limit_cpu, 'memory': mcrouter_limit_memory})
    mcrouter_container = client.V1Container(
        name='mcrouter',
        command=[
            'mcrouter', '-p', '11211', '-f', '/etc/mcrouter/mcrouter.conf'],
        image='kubestack/mcrouter:v0.36.0-kbst1',
        ports=[mcrouter_port],
        volume_mounts=[mcrouter_config_volumemount],
        resources=mcrouter_resources)

    # Mcrouter config sidecar
    sidecar_resources = client.V1ResourceRequirements(
        limits={'cpu': '25m', 'memory': '8Mi'},
        requests={'cpu': '25m', 'memory': '8Mi'})
    sidecar_config_volumemount = client.V1VolumeMount(
        name='mcrouter-config',
        read_only=True,
        mount_path='/etc/mcrouter')
    sidecar_container = client.V1Container(
        name='config-sidecar',
        args=[
            "--debug",
            "--output=/etc/mcrouter/mcrouter.conf",
            "{}-backend.{}.svc.cluster.local".format(name, namespace)],
        image='kubestack/mcrouter_sidecar:v0.1.0',
        volume_mounts=[mcrouter_config_volumemount],
        resources=sidecar_resources)

    # Config Map Volume
    mcrouter_config_volume = client.V1Volume(
        name='mcrouter-config',
        empty_dir=client.V1EmptyDirVolumeSource())
    deployment.spec.template.spec.volumes = [mcrouter_config_volume]

    # Metrics container
    metrics_port = client.V1ContainerPort(
        name='metrics', container_port=9150, protocol='TCP')
    metrics_resources = client.V1ResourceRequirements(
        limits={'cpu': '50m', 'memory': '16Mi'},
        requests={'cpu': '50m', 'memory': '16Mi'})
    metrics_container = client.V1Container(
        name='prometheus-exporter',
        image='kubestack/mcrouter_exporter:v0.0.1',
        args=[
            '-mcrouter.address', 'localhost:11211',
            '-web.listen-address', ':9150'],
        ports=[metrics_port],
        resources=metrics_resources)

    deployment.spec.template.spec.containers = [
        mcrouter_container, sidecar_container, metrics_container]
    return deployment
