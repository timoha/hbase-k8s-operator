# HBase Kubernetes Operator

HBase Kubernetes Operator automates the deployment, provisioning, and orchestration of HBase running on Kubernetes based on [Operator Framework](https://operatorframework.io/) pattern.

Current features:

- Provisions a service, a config, masters, and regionservers
- Graceful rolling upgrade when any of the config or pod specs change

Current limitations:
- Operates only on healthy clusters, manual intervention is required in case of issues (it's the job of HBase to recover from failures)
- Graceful scaling down of regionservers isn't supported

Installation instructions:
1. run `make install` to tell k8s how to understand the Custom HBase Resource (CRD)
2. run `make deploy IMG=timoha/hbase-k8s-controller:latest` to deploy the operator
3. modify [sample HBase CRD](https://github.com/timoha/hbase-k8s-operator/config/samples/hbase_v1_hbase.yaml) to have the desired config
4. run `kubectl apply -f config/samples/hbase_v1_hbase.yaml` to tell the operator what to deploy.
5. now you can go and focus on actual features of your product

The perator and HBase must be deployed to the same namespace, which defaults to `hbase`.
