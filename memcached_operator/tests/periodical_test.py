from unittest.mock import patch, call, MagicMock
from copy import deepcopy

from kubernetes import client

from ..memcached_operator.periodical import (is_version_cached, cache_version,
                                             check_existing, collect_garbage)
from ..memcached_operator.kubernetes_resources import (
                                                get_mcrouter_service_object,
                                                get_memcached_service_object)


class TestVersionCache():
    def setUp(self):
        self.uid = 'test-uid-1234567890'
        self.version = '123'
        self.resource = MagicMock()
        self.resource.metadata.uid = self.uid
        self.resource.metadata.version = self.version

    def test_version_not_cached(self):
        result = is_version_cached(self.resource)

        assert result is False

    def test_version_cached(self):
        cache_version(self.resource)
        result = is_version_cached(self.resource)

        assert result is True


class TestCheckExisting():
    def setUp(self):
        self.name = 'testname123'
        self.namespace = 'testnamespace456'
        self.cluster_object = {'metadata':{'name': self.name,
                                           'namespace': self.namespace}}
        self.base_list_result = {'items': [self.cluster_object]}

    @patch('memcached_operator.memcached_operator.periodical.logging')
    @patch('memcached_operator.memcached_operator.periodical.list_cluster_memcached_object', side_effect=client.rest.ApiException())
    def test_list_memcached_exception(self, mock_list_cluster_memcached_object, mock_logging):
        result = check_existing()

        assert mock_logging.exception.called is True
        assert result is False

    @patch('memcached_operator.memcached_operator.periodical.update_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.update_mcrouter_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_mcrouter_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.read_namespaced_deployment')
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached')
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service')
    @patch('memcached_operator.memcached_operator.periodical.list_cluster_memcached_object')
    def test_no_memcached_tprs(self, mock_list_cluster_memcached_object, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_mcrouter_deployment, mock_create_memcached_deployment, mock_update_mcrouter_deployment, mock_update_memcached_deployment):
        # Mock list memcached call with 0 items
        no_item_result = deepcopy(self.base_list_result)
        no_item_result['items'] = []
        mock_list_cluster_memcached_object.return_value = no_item_result

        check_existing()

        mock_list_cluster_memcached_object.assert_called_once_with()
        assert mock_read_namespaced_service.called is False
        assert mock_create_service.called is False
        assert mock_cache_version.called is False
        assert mock_is_version_cached.called is False
        assert mock_update_service.called is False
        assert mock_read_namespaced_deployment.called is False
        assert mock_create_memcached_deployment.called is False
        assert mock_create_mcrouter_deployment.called is False
        assert mock_update_memcached_deployment.called is False
        assert mock_update_memcached_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.update_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.update_mcrouter_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_memcached_deployment', return_value=client.AppsV1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.create_mcrouter_deployment', return_value=client.AppsV1beta1Deployment())
    @patch('kubernetes.client.AppsV1beta1Api.read_namespaced_deployment', side_effect=client.rest.ApiException(status=404))
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=True)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service', return_value=client.V1Service())
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', side_effect=client.rest.ApiException(status=404))
    @patch('memcached_operator.memcached_operator.periodical.list_cluster_memcached_object')
    def test_service_and_deploy_404(self, mock_list_cluster_memcached_object, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_mcrouter_deployment, mock_create_memcached_deployment, mock_update_mcrouter_deployment, mock_update_memcached_deployment):
        # Mock list memcached call with 0 items
        mock_list_cluster_memcached_object.return_value = self.base_list_result

        check_existing()

        mock_list_cluster_memcached_object.assert_called_once_with()

        service_calls = [
            call(self.name, self.namespace),
            call('{}-backend'.format(self.name), self.namespace)]
        mock_read_namespaced_service.assert_has_calls(service_calls)

        create_service_calls = [
            call(get_mcrouter_service_object(self.cluster_object)),
            call(get_memcached_service_object(self.cluster_object))]
        mock_create_service.assert_has_calls(create_service_calls)

        cache_version_calls = [
            call(client.V1Service()),
            call(client.AppsV1beta1Deployment())]
        mock_cache_version.assert_has_calls(cache_version_calls)

        assert mock_is_version_cached.called is False
        assert mock_update_service.called is False
        read_namespaced_deployment_calls = [
            call(self.name, self.namespace),
            call('{}-router'.format(self.name), self.namespace)]
        mock_read_namespaced_deployment.assert_has_calls(read_namespaced_deployment_calls)
        mock_create_memcached_deployment.assert_called_once_with(self.cluster_object)
        mock_create_mcrouter_deployment.assert_called_once_with(self.cluster_object)
        assert mock_update_memcached_deployment.called is False
        assert mock_update_mcrouter_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.update_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.update_mcrouter_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_memcached_deployment', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.create_mcrouter_deployment', return_value=False)
    @patch('kubernetes.client.AppsV1beta1Api.read_namespaced_deployment', side_effect=client.rest.ApiException(status=404))
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=True)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service', return_value=False)
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', side_effect=client.rest.ApiException(status=404))
    @patch('memcached_operator.memcached_operator.periodical.list_cluster_memcached_object')
    def test_service_and_deploy_404_yet_create_false(self, mock_list_cluster_memcached_object, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_mcrouter_deployment, mock_create_memcached_deployment, mock_update_mcrouter_deployment, mock_update_memcached_deployment):
        # Mock list memcached call with 0 items
        mock_list_cluster_memcached_object.return_value = self.base_list_result

        check_existing()

        mock_list_cluster_memcached_object.assert_called_once_with()

        read_service_calls = [
            call(self.name, self.namespace),
            call('{}-backend'.format(self.name), self.namespace)]
        mock_read_namespaced_service.assert_has_calls(read_service_calls)

        create_service_calls = [
            call(get_mcrouter_service_object(self.cluster_object)),
            call(get_memcached_service_object(self.cluster_object))]
        mock_create_service.assert_has_calls(create_service_calls)

        assert mock_cache_version.called is False

        assert mock_is_version_cached.called is False
        assert mock_update_service.called is False
        read_namespaced_deployment_calls = [
            call(self.name, self.namespace),
            call('{}-router'.format(self.name), self.namespace)]
        mock_read_namespaced_deployment.assert_has_calls(read_namespaced_deployment_calls)
        mock_create_memcached_deployment.assert_called_once_with(self.cluster_object)
        mock_create_mcrouter_deployment.assert_called_once_with(self.cluster_object)
        assert mock_update_memcached_deployment.called is False
        assert mock_update_mcrouter_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.logging')
    @patch('memcached_operator.memcached_operator.periodical.update_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.update_mcrouter_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_mcrouter_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.read_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached')
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', side_effect=client.rest.ApiException(status=500))
    @patch('memcached_operator.memcached_operator.periodical.list_cluster_memcached_object')
    def test_service_and_deploy_500(self, mock_list_cluster_memcached_object, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_mcrouter_deployment, mock_create_memcached_deployment, mock_update_mcrouter_deployment, mock_update_memcached_deployment, mock_logging):
        # Mock list memcached call
        mock_list_cluster_memcached_object.return_value = self.base_list_result

        check_existing()

        mock_list_cluster_memcached_object.assert_called_once_with()
        read_namespaced_service_calls = [
            call(self.name, self.namespace),
            call('{}-backend'.format(self.name), self.namespace)]
        mock_read_namespaced_service.assert_has_calls(read_namespaced_service_calls)
        assert mock_create_service.called is False
        assert mock_cache_version.called is False
        assert mock_is_version_cached.called is False
        assert mock_update_service.called is False
        read_namespaced_deployment_calls = [
            call(self.name, self.namespace),
            call('{}-router'.format(self.name), self.namespace)]
        mock_read_namespaced_deployment.assert_has_calls(read_namespaced_deployment_calls)
        assert mock_create_memcached_deployment.called is False
        assert mock_create_mcrouter_deployment.called is False
        assert mock_update_memcached_deployment.called is False
        assert mock_update_mcrouter_deployment.called is False
        mock_logging.exception.call_count == 2

    @patch('memcached_operator.memcached_operator.periodical.update_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.update_mcrouter_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_mcrouter_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.read_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=True)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', return_value=client.V1Service())
    @patch('memcached_operator.memcached_operator.periodical.list_cluster_memcached_object')
    def test_service_and_deploy_cached(self, mock_list_cluster_memcached_object, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_mcrouter_deployment, mock_create_memcached_deployment, mock_update_mcrouter_deployment, mock_update_memcached_deployment):
        # Mock list memcached call with 0 items
        mock_list_cluster_memcached_object.return_value = self.base_list_result

        check_existing()

        mock_list_cluster_memcached_object.assert_called_once_with()
        read_namespaced_service_calls = [
            call(self.name, self.namespace),
            call('{}-backend'.format(self.name), self.namespace)]
        mock_read_namespaced_service.assert_has_calls(read_namespaced_service_calls)
        assert mock_create_service.called is False
        assert mock_cache_version.called is False

        is_version_cached_calls = [
            call(client.V1Service()),
            call(client.AppsV1beta1Deployment())]
        mock_is_version_cached.assert_has_calls(is_version_cached_calls)

        assert mock_update_service.called is False
        read_namespaced_deployment_calls = [
            call(self.name, self.namespace),
            call('{}-router'.format(self.name), self.namespace)]
        mock_read_namespaced_deployment.assert_has_calls(read_namespaced_deployment_calls)
        assert mock_create_memcached_deployment.called is False
        assert mock_create_mcrouter_deployment.called is False
        assert mock_update_memcached_deployment.called is False
        assert mock_update_mcrouter_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.update_memcached_deployment', return_value=client.AppsV1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.update_mcrouter_deployment', return_value=client.AppsV1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.create_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_mcrouter_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.read_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.update_service', return_value=client.V1Service())
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', return_value=client.V1Service())
    @patch('memcached_operator.memcached_operator.periodical.list_cluster_memcached_object')
    def test_service_and_deploy_not_cached(self, mock_list_cluster_memcached_object, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_mcrouter_deployment, mock_create_memcached_deployment, mock_update_mcrouter_deployment, mock_update_memcached_deployment):
        # Mock list memcached call with 0 items
        mock_list_cluster_memcached_object.return_value = self.base_list_result

        check_existing()

        mock_list_cluster_memcached_object.assert_called_once_with()

        read_namespaced_service_calls = [
            call(self.name, self.namespace),
            call('{}-backend'.format(self.name), self.namespace)]
        mock_read_namespaced_service.assert_has_calls(read_namespaced_service_calls)
        assert mock_create_service.called is False

        cache_version_calls = [
            call(client.V1Service()),
            call(client.AppsV1beta1Deployment())]
        mock_cache_version.assert_has_calls(cache_version_calls)

        is_version_cached_calls = [
            call(client.V1Service()),
            call(client.AppsV1beta1Deployment())]
        mock_is_version_cached.assert_has_calls(is_version_cached_calls)

        update_service_calls = [
            call(get_mcrouter_service_object(self.cluster_object)),
            call(get_memcached_service_object(self.cluster_object))]
        mock_update_service.assert_has_calls(update_service_calls)
        read_namespaced_deployment_calls = [
            call(self.name, self.namespace),
            call('{}-router'.format(self.name), self.namespace)]
        mock_read_namespaced_deployment.assert_has_calls(read_namespaced_deployment_calls)
        assert mock_create_memcached_deployment.called is False
        assert mock_create_mcrouter_deployment.called is False
        mock_update_memcached_deployment.assert_called_once_with(self.cluster_object)
        mock_update_mcrouter_deployment.assert_called_once_with(self.cluster_object)

    @patch('memcached_operator.memcached_operator.periodical.update_memcached_deployment', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.update_mcrouter_deployment', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.create_memcached_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_mcrouter_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.read_namespaced_deployment', return_value=client.AppsV1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.update_service', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', return_value=client.V1Service())
    @patch('memcached_operator.memcached_operator.periodical.list_cluster_memcached_object')
    def test_service_and_deploy_not_cached_yet_update_exception(self, mock_list_cluster_memcached_object, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_mcrouter_deployment, mock_create_memcached_deployment, mock_update_mcrouter_deployment, mock_update_memcached_deployment):
        # Mock list memcached call with 0 items
        mock_list_cluster_memcached_object.return_value = self.base_list_result

        check_existing()

        mock_list_cluster_memcached_object.assert_called_once_with()

        read_namespaced_service_calls = [
            call(self.name, self.namespace),
            call('{}-backend'.format(self.name), self.namespace)]
        mock_read_namespaced_service.assert_has_calls(read_namespaced_service_calls)
        assert mock_create_service.called is False

        assert mock_cache_version.called is False

        is_version_cached_calls = [
            call(client.V1Service()),
            call(client.AppsV1beta1Deployment())]
        mock_is_version_cached.assert_has_calls(is_version_cached_calls)

        update_service_calls = [
            call(get_mcrouter_service_object(self.cluster_object)),
            call(get_memcached_service_object(self.cluster_object))]
        mock_update_service.assert_has_calls(update_service_calls)
        read_namespaced_deployment_calls = [
            call(self.name, self.namespace),
            call('{}-router'.format(self.name), self.namespace)]
        mock_read_namespaced_deployment.assert_has_calls(read_namespaced_deployment_calls)
        assert mock_create_memcached_deployment.called is False
        assert mock_create_mcrouter_deployment.called is False
        mock_update_memcached_deployment.assert_called_once_with(self.cluster_object)
        mock_update_mcrouter_deployment.assert_called_once_with(self.cluster_object)


class TestCollectGargabe():
    def setUp(self):
        self.name = 'testname123'
        self.namespace = 'testnamespace456'

        svc_list = client.V1ServiceList(items=[])
        svc = client.V1Service()
        svc.metadata = client.V1ObjectMeta(
            name=self.name, namespace=self.namespace)
        svc.metadata.labels = {
            'operated-by': 'memcached.operator.kubestack.com',
            'heritage': 'kubestack.com',
            'cluster': self.name}
        svc_list.items = [svc]
        self.correct_svc_list = svc_list

        cm_list = client.V1ConfigMapList(items=[])
        cm = client.V1ConfigMap()
        cm.metadata = client.V1ObjectMeta(
            name=self.name, namespace=self.namespace)
        cm.metadata.labels = {
            'operated-by': 'memcached.operator.kubestack.com',
            'heritage': 'kubestack.com',
            'cluster': self.name}
        cm_list.items = [cm]
        self.correct_cm_list = cm_list

        deploy_list = client.AppsV1beta1DeploymentList(items=[])
        memcached_deploy = client.AppsV1beta1Deployment()
        memcached_deploy.metadata = client.V1ObjectMeta(
            name=self.name, namespace=self.namespace)
        memcached_deploy.metadata.labels = {
            'operated-by': 'memcached.operator.kubestack.com',
            'heritage': 'kubestack.com',
            'cluster': self.name}
        mcrouter_deploy = client.AppsV1beta1Deployment()
        mcrouter_deploy.metadata = client.V1ObjectMeta(
            name='{}-router'.format(self.name), namespace=self.namespace)
        mcrouter_deploy.metadata.labels = {
            'operated-by': 'memcached.operator.kubestack.com',
            'heritage': 'kubestack.com',
            'cluster': self.name}
        deploy_list.items = [memcached_deploy, mcrouter_deploy]
        self.correct_deploy_list = deploy_list

    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.periodical.get_namespaced_memcached_object')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_services_and_deployments_exceptions(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_get_namespaced_memcached_object, mock_list_deployment_for_all_namespaces, mock_reap_deployment):
        # Mock service list exception
        mock_list_service_for_all_namespaces.side_effect = client.rest.ApiException()

        # Mock deployment list exception
        mock_list_deployment_for_all_namespaces.side_effect = client.rest.ApiException()

        collect_garbage()
        assert mock_get_namespaced_memcached_object.called is False
        assert mock_delete_service.called is False
        assert mock_reap_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.periodical.get_namespaced_memcached_object')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_no_services_and_deployments(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_get_namespaced_memcached_object, mock_list_deployment_for_all_namespaces, mock_reap_deployment):
        # Mock emtpy service list
        empty_svc_list = deepcopy(self.correct_svc_list)
        empty_svc_list.items = []
        mock_list_service_for_all_namespaces.return_value = empty_svc_list

        # Mock emtpy deployment list
        empty_deploy_list = deepcopy(self.correct_deploy_list)
        empty_deploy_list.items = []
        mock_list_deployment_for_all_namespaces.return_value = empty_deploy_list

        collect_garbage()
        assert mock_get_namespaced_memcached_object.called is False
        assert mock_delete_service.called is False
        assert mock_reap_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.periodical.get_namespaced_memcached_object')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_expected_services_and_deployments(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_get_namespaced_memcached_object, mock_list_deployment_for_all_namespaces, mock_reap_deployment):
        # Mock service list
        mock_list_service_for_all_namespaces.return_value = self.correct_svc_list

        # Mock deployment list
        mock_list_deployment_for_all_namespaces.return_value = self.correct_deploy_list

        collect_garbage()
        read_namespaced_memcached_calls = [
            call(self.name, self.namespace),
            call(self.name, self.namespace),
            call(self.name, self.namespace)]
        mock_get_namespaced_memcached_object.assert_has_calls(
            read_namespaced_memcached_calls)
        assert mock_delete_service.called is False
        assert mock_reap_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.periodical.get_namespaced_memcached_object')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_unexpected_services_and_deployments(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_get_namespaced_memcached_object, mock_list_deployment_for_all_namespaces, mock_reap_deployment):
        # Mock service list
        mock_list_service_for_all_namespaces.return_value = self.correct_svc_list

        # Mock deployment list
        mock_list_deployment_for_all_namespaces.return_value = self.correct_deploy_list

        # Mock read namespaced memcached side effect
        mock_get_namespaced_memcached_object.side_effect = client.rest.ApiException(status=404)

        collect_garbage()
        read_namespaced_memcached_calls = [
            call(self.name, self.namespace),
            call(self.name, self.namespace),
            call(self.name, self.namespace)]
        mock_get_namespaced_memcached_object.assert_has_calls(
            read_namespaced_memcached_calls)
        mock_delete_service.assert_called_once_with(self.name, self.namespace)
        reap_deployment_calls = [
            call(self.name, self.namespace),
            call('{}-router'.format(self.name), self.namespace)]
        mock_reap_deployment.assert_has_calls(reap_deployment_calls)

    @patch('memcached_operator.memcached_operator.periodical.logging')
    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.AppsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.periodical.get_namespaced_memcached_object')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_read_services_and_deployments_500(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_get_namespaced_memcached_object, mock_list_deployment_for_all_namespaces, mock_reap_deployment, mock_logging):
        # Mock service list
        mock_list_service_for_all_namespaces.return_value = self.correct_svc_list

        # Mock deployment list
        mock_list_deployment_for_all_namespaces.return_value = self.correct_deploy_list

        # Mock read namespaced memcached side effect
        mock_get_namespaced_memcached_object.side_effect = client.rest.ApiException(status=500)

        collect_garbage()
        read_namespaced_memcached_calls = [
            call(self.name, self.namespace),
            call(self.name, self.namespace),
            call(self.name, self.namespace)]
        mock_get_namespaced_memcached_object.assert_has_calls(
            read_namespaced_memcached_calls)
        assert mock_delete_service.called is False
        assert mock_reap_deployment.called is False
        assert mock_logging.exception.called is True
        assert mock_logging.exception.call_count == 3
