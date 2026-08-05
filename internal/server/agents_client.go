package server

import (
	"context"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	"google.golang.org/grpc"
)

type agentsClient interface {
	GetAgent(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	GetSandbox(ctx context.Context, req *agentsv1.GetSandboxRequest, opts ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error)
	GetVolume(ctx context.Context, req *agentsv1.GetVolumeRequest, opts ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error)
	ListVolumes(ctx context.Context, req *agentsv1.ListVolumesRequest, opts ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error)
	ListVolumeAttachments(ctx context.Context, req *agentsv1.ListVolumeAttachmentsRequest, opts ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error)
	GetMcp(ctx context.Context, req *agentsv1.GetMcpRequest, opts ...grpc.CallOption) (*agentsv1.GetMcpResponse, error)
}
