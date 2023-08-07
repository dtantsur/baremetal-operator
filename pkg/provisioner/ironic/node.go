package ironic

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack/baremetal/v1/nodes"
	"github.com/gophercloud/gophercloud/openstack/baremetal/v1/ports"
)

// Node is a convenience abstraction around the Node API.
type Node struct {
	nodes.Node
	log     logr.Logger
	client  *gophercloud.ServiceClient
	updater *nodeUpdater
}

// GetNode returns a node by its ID or nil.
func GetNode(client *gophercloud.ServiceClient, log logr.Logger, nodeID string) (*Node, error) {
	if nodeID == "" {
		return nil, nil
	}

	node := Node{
		log:     log.WithValues("NodeID", nodeID),
		client:  client,
		updater: updateOptsBuilder(log),
	}
	err := nodes.Get(client, nodeID).ExtractInto(&node.Node)
	switch err.(type) {
	case nil:
		node.log.V(1).Info("found existing node by ID")
		return &node, nil
	case gophercloud.ErrDefault404:
		return nil, nil
	default:
		return nil, fmt.Errorf("failed to find node by ID %s: %w", nodeID, err)
	}
}

// AssertNode returns a node by its ID and fails if it is not found.
// The returned Node is never nil.
func AssertNode(client *gophercloud.ServiceClient, log logr.Logger, nodeID string) (*Node, error) {
	node, err := GetNode(client, log, nodeID)
	if node == nil && err != nil {
		err = fmt.Errorf("failed to find node by ID %s: not found", nodeID)
	}

	return node, err
}

// FindNodeByNames finds a node by one or more possible names.
// The first match is returned. This function does not check for duplicates.
func FindNodeByNames(client *gophercloud.ServiceClient, log logr.Logger, names []string) (*Node, error) {
	debugLog := log.V(1)
	for _, nodeName := range names {
		debugLog.Info("looking for existing node by name", "name", nodeName)
		node, err := GetNode(client, log, nodeName)
		if err != nil {
			return nil, fmt.Errorf("failed to find node by name %s: %w", nodeName, err)
		}
		if node != nil {
			debugLog.Info("found existing node by name", "name", nodeName)
			return node, nil
		}

		log.Info(fmt.Sprintf("node with name %s doesn't exist", nodeName))
	}

	return nil, nil
}

// findNodeIDByMAC returns node ID matching the MAC address or an empty string.
func findNodeIDByMAC(client *gophercloud.ServiceClient, log logr.Logger, macAddress string) (string, error) {
	opts := ports.ListOpts{
		Fields:  []string{"node_uuid"},
		Address: macAddress,
	}

	pages, err := ports.List(client, opts).AllPages()
	if err != nil {
		return "", err
	}

	ports, err := ports.ExtractPorts(pages)
	if err != nil {
		return "", err
	}

	if len(ports) == 0 {
		return "", nil
	}

	// MAC address is unique in Ironic, so only one port can be present here.
	return ports[0].NodeUUID, nil
}

// FindNodeByMAC returns a node by one of its MAC addresses.
func FindNodeByMAC(client *gophercloud.ServiceClient, log logr.Logger, macAddress string) (*Node, error) {
	nodeID, err := findNodeIDByMAC(client, log, macAddress)
	if nodeID == "" || err != nil {
		return nil, err
	}

	return GetNode(client, log, nodeID)
}

// CreateBootPort creates a port with PXE booting enabled.
func (node *Node) CreateBootPort(macAddress string) error {
	node.log.Info("creating PXE enabled ironic port for node", "NodeUUID", node.UUID, "MAC", macAddress)

	enabled := true
	_, err := ports.Create(
		node.client,
		ports.CreateOpts{
			NodeUUID:   node.UUID,
			Address:    macAddress,
			PXEEnabled: &enabled,
		}).Extract()
	if err != nil {
		return fmt.Errorf("failed to create ironic port %s for node %s: %w", macAddress, node.UUID, err)
	}

	return nil
}

// Validate validates boot and deploy information for the node.
// Validation failures are returned as a list of strings, while failures to run the validation call itself are returned as a normal error.
func (node *Node) Validate() (failures []string, err error) {
	node.log.Info("validating node settings in ironic")

	validateResult, err := nodes.Validate(node.client, node.UUID).Extract()
	if err != nil {
		return // do not wrap error so we can check type in caller
	}

	if !validateResult.Boot.Result {
		failures = append(failures, validateResult.Boot.Reason)
	}
	if !validateResult.Deploy.Result {
		failures = append(failures, validateResult.Deploy.Reason)
	}
	return

}

// HasPorts checks whether the node has any ports.
func (node *Node) HasPorts() (bool, error) {
	opts := ports.ListOpts{
		Fields:   []string{"node_uuid"},
		NodeUUID: node.UUID,
	}

	pager := ports.List(node.client, opts)

	allPages, err := pager.AllPages()
	if err != nil {
		return false, fmt.Errorf("failed to page over list of ports: %w", err)
	}

	empty, err := allPages.IsEmpty()
	if err != nil {
		return false, fmt.Errorf("failed to check port list status: %w", err)
	}

	if empty {
		return false, nil
	}

	return true, nil
}
