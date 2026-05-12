package trafficmgr

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
)

func normalizeMappedNamespaces(namespaces []string) []string {
	if len(namespaces) == 1 && namespaces[0] == "all" {
		return nil
	}
	namespaces = slices.Clone(namespaces)
	sort.Strings(namespaces)
	return slices.Compact(namespaces)
}

func mappedNamespacesAll(namespaces []string) bool {
	return len(namespaces) == 1 && namespaces[0] == "all"
}

func effectiveMappedNamespaces(requestedNamespaces, clientNamespaces, managerNamespaces []string) ([]string, error) {
	requestedAll := mappedNamespacesAll(requestedNamespaces)
	requestedNamespaces = normalizeMappedNamespaces(requestedNamespaces)
	clientAll := mappedNamespacesAll(clientNamespaces)
	clientNamespaces = normalizeMappedNamespaces(clientNamespaces)
	managerNamespaces = normalizeMappedNamespaces(managerNamespaces)

	var namespaces []string
	switch {
	case requestedAll:
		namespaces = managerNamespaces
	case len(requestedNamespaces) > 0:
		namespaces = requestedNamespaces
	case clientAll:
		namespaces = managerNamespaces
	case len(clientNamespaces) > 0:
		namespaces = clientNamespaces
	case len(managerNamespaces) > 0:
		namespaces = managerNamespaces
	}
	if err := ensureMappedNamespacesManaged(namespaces, managerNamespaces); err != nil {
		return nil, err
	}
	return namespaces, nil
}

func ensureMappedNamespacesManaged(requestedNamespaces, managerNamespaces []string) error {
	requestedNamespaces = normalizeMappedNamespaces(requestedNamespaces)
	managerNamespaces = normalizeMappedNamespaces(managerNamespaces)
	if len(requestedNamespaces) == 0 || len(managerNamespaces) == 0 {
		return nil
	}

	managed := make(map[string]struct{}, len(managerNamespaces))
	for _, namespace := range managerNamespaces {
		managed[namespace] = struct{}{}
	}

	var unmanaged []string
	for _, namespace := range requestedNamespaces {
		if _, ok := managed[namespace]; !ok {
			unmanaged = append(unmanaged, namespace)
		}
	}
	if len(unmanaged) == 0 {
		return nil
	}

	return errcat.User.Newf(
		"mapped %s %s not managed by this traffic-manager; managed namespaces are %s. Reconnect with --mapped-namespaces set to managed namespaces, or reconfigure the traffic-manager to manage %s",
		pluralize("namespace", len(unmanaged)),
		quotedList(unmanaged),
		quotedList(managerNamespaces),
		quotedList(unmanaged),
	)
}

func pluralize(word string, count int) string {
	if count == 1 {
		return word
	}
	return word + "s"
}

func quotedList(values []string) string {
	if len(values) == 0 {
		return "<none>"
	}
	values = normalizeMappedNamespaces(values)
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = fmt.Sprintf("%q", value)
	}
	return strings.Join(quoted, ", ")
}
