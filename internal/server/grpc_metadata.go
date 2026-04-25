package server

import (
	"context"

	"google.golang.org/grpc/metadata"
)

func outgoingContext(ctx context.Context) context.Context {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	if outgoing, ok := metadata.FromOutgoingContext(ctx); ok {
		return metadata.NewOutgoingContext(ctx, metadata.Join(outgoing, incoming))
	}
	return metadata.NewOutgoingContext(ctx, incoming)
}
