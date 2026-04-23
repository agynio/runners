package zitimanager

import (
	"context"
	"testing"

	zitimgmtv1 "github.com/agynio/runners/.gen/go/agynio/api/ziti_management/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeZitiManagementClient struct {
	requestServiceIdentity func(ctx context.Context, req *zitimgmtv1.RequestServiceIdentityRequest) (*zitimgmtv1.RequestServiceIdentityResponse, error)
}

func (f fakeZitiManagementClient) CreateAgentIdentity(ctx context.Context, req *zitimgmtv1.CreateAgentIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateAppIdentity(ctx context.Context, req *zitimgmtv1.CreateAppIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateService(ctx context.Context, req *zitimgmtv1.CreateServiceRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateServiceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteIdentity(ctx context.Context, req *zitimgmtv1.DeleteIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteAppIdentity(ctx context.Context, req *zitimgmtv1.DeleteAppIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateRunnerIdentity(ctx context.Context, req *zitimgmtv1.CreateRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateRunnerIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteRunnerIdentity(ctx context.Context, req *zitimgmtv1.DeleteRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteRunnerIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ListManagedIdentities(ctx context.Context, req *zitimgmtv1.ListManagedIdentitiesRequest, opts ...grpc.CallOption) (*zitimgmtv1.ListManagedIdentitiesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ResolveIdentity(ctx context.Context, req *zitimgmtv1.ResolveIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.ResolveIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) RequestServiceIdentity(ctx context.Context, req *zitimgmtv1.RequestServiceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
	if f.requestServiceIdentity == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.requestServiceIdentity(ctx, req)
}

func (f fakeZitiManagementClient) ExtendIdentityLease(ctx context.Context, req *zitimgmtv1.ExtendIdentityLeaseRequest, opts ...grpc.CallOption) (*zitimgmtv1.ExtendIdentityLeaseResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateServicePolicy(ctx context.Context, req *zitimgmtv1.CreateServicePolicyRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateServicePolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteServicePolicy(ctx context.Context, req *zitimgmtv1.DeleteServicePolicyRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteServicePolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteService(ctx context.Context, req *zitimgmtv1.DeleteServiceRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteServiceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateDeviceIdentity(ctx context.Context, req *zitimgmtv1.CreateDeviceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.CreateDeviceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteDeviceIdentity(ctx context.Context, req *zitimgmtv1.DeleteDeviceIdentityRequest, opts ...grpc.CallOption) (*zitimgmtv1.DeleteDeviceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func TestRequestServiceIdentityWithFallback(t *testing.T) {
	var calls []zitimgmtv1.ServiceType
	client := fakeZitiManagementClient{
		requestServiceIdentity: func(ctx context.Context, req *zitimgmtv1.RequestServiceIdentityRequest) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
			calls = append(calls, req.GetServiceType())
			switch req.GetServiceType() {
			case zitimgmtv1.ServiceType_SERVICE_TYPE_RUNNERS:
				return nil, status.Error(codes.InvalidArgument, "unknown service type 6")
			case zitimgmtv1.ServiceType_SERVICE_TYPE_ORCHESTRATOR:
				return &zitimgmtv1.RequestServiceIdentityResponse{
					ZitiIdentityId: "identity-1",
					IdentityJson:   []byte("{}"),
				}, nil
			default:
				return nil, status.Errorf(codes.InvalidArgument, "unexpected service type %v", req.GetServiceType())
			}
		},
	}

	manager := &Manager{mgmtClient: client}
	resp, err := manager.requestServiceIdentityWithFallback(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetZitiIdentityId() != "identity-1" {
		t.Fatalf("expected identity id 'identity-1', got %q", resp.GetZitiIdentityId())
	}
	if len(calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(calls))
	}
	if calls[0] != zitimgmtv1.ServiceType_SERVICE_TYPE_RUNNERS {
		t.Fatalf("expected first call to runners, got %v", calls[0])
	}
	if calls[1] != zitimgmtv1.ServiceType_SERVICE_TYPE_ORCHESTRATOR {
		t.Fatalf("expected fallback to orchestrator, got %v", calls[1])
	}
}

func TestRequestServiceIdentityWithoutFallback(t *testing.T) {
	var calls []zitimgmtv1.ServiceType
	client := fakeZitiManagementClient{
		requestServiceIdentity: func(ctx context.Context, req *zitimgmtv1.RequestServiceIdentityRequest) (*zitimgmtv1.RequestServiceIdentityResponse, error) {
			calls = append(calls, req.GetServiceType())
			return &zitimgmtv1.RequestServiceIdentityResponse{
				ZitiIdentityId: "identity-2",
				IdentityJson:   []byte("{}"),
			}, nil
		},
	}

	manager := &Manager{mgmtClient: client}
	resp, err := manager.requestServiceIdentityWithFallback(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetZitiIdentityId() != "identity-2" {
		t.Fatalf("expected identity id 'identity-2', got %q", resp.GetZitiIdentityId())
	}
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0] != zitimgmtv1.ServiceType_SERVICE_TYPE_RUNNERS {
		t.Fatalf("expected runners service type, got %v", calls[0])
	}
}
