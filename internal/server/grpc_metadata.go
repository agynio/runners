package server

import (
	"context"

	"google.golang.org/grpc/metadata"
)

var forwardedMetadataKeys = []string{
	identityMetadata,
	"x-identity-type",
	"x-organization-id",
}

func outgoingContext(ctx context.Context) context.Context {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	forwarded := metadata.MD{}
	for _, key := range forwardedMetadataKeys {
		values := incoming.Get(key)
		if len(values) == 0 {
			continue
		}
		forwarded[key] = append([]string(nil), values...)
	}
	if len(forwarded) == 0 {
		return ctx
	}
	if outgoing, ok := metadata.FromOutgoingContext(ctx); ok {
		merged := outgoing.Copy()
		for key, values := range forwarded {
			if len(merged.Get(key)) > 0 {
				continue
			}
			merged[key] = append([]string(nil), values...)
		}
		return metadata.NewOutgoingContext(ctx, merged)
	}
	return metadata.NewOutgoingContext(ctx, forwarded)
}
