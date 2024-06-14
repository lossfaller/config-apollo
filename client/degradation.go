package client

import (
	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/kitex-contrib/config-apollo/apollo"
	"github.com/kitex-contrib/config-apollo/model"
	"github.com/kitex-contrib/config-apollo/utils"
)

func WithDegradation(dest, src string, apolloClient apollo.Client,
	opts utils.Options,
) []client.Option {
	param, err := apolloClient.ClientConfigParam(&apollo.ConfigParamConfig{
		Category:          apollo.DegradationConfigName,
		ServerServiceName: dest,
		ClientServiceName: src,
	})
	if err != nil {
		panic(err)
	}
	for _, f := range opts.ApolloCustomFunctions {
		f(&param)
	}
	uniqueID := apollo.GetUniqueID()

	container := initDegradationOptions(param, dest, uniqueID, apolloClient)

	return []client.Option{
		client.WithACLRules(container.GetAclRule()),
		client.WithCloseCallbacks(func() error {
			err := apolloClient.DeregisterConfig(param, uniqueID)
			if err != nil {
				return err
			}
			return nil
		}),
	}
}

func initDegradationOptions(param apollo.ConfigParam, dest string, uniqueID int64, apolloClient apollo.Client) *model.Container {
	container := model.NewContainer()
	onChangeCallback := func(data string, parser apollo.ConfigParser) {
		cfg := &model.Config{}
		err := parser.Decode(param.Type, data, &cfg)
		if err != nil {
			klog.Warnf("[etcd] %s server etcd degradation config: unmarshal data %s failed: %s, skip...", dest, data, err)
			return
		}
		container.NotifyPolicyChange(cfg)
	}
	apolloClient.RegisterConfigCallback(param, onChangeCallback, uniqueID)
	return container
}
