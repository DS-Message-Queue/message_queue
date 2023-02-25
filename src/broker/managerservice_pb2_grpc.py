# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import managerservice_pb2 as managerservice__pb2


class ManagerServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.RegisterBroker = channel.unary_unary(
                '/managerservice.ManagerService/RegisterBroker',
                request_serializer=managerservice__pb2.BrokerDetails.SerializeToString,
                response_deserializer=managerservice__pb2.Status.FromString,
                )
        self.HealthCheck = channel.unary_unary(
                '/managerservice.ManagerService/HealthCheck',
                request_serializer=managerservice__pb2.HeartBeat.SerializeToString,
                response_deserializer=managerservice__pb2.HeartBeat.FromString,
                )


class ManagerServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def RegisterBroker(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def HealthCheck(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ManagerServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'RegisterBroker': grpc.unary_unary_rpc_method_handler(
                    servicer.RegisterBroker,
                    request_deserializer=managerservice__pb2.BrokerDetails.FromString,
                    response_serializer=managerservice__pb2.Status.SerializeToString,
            ),
            'HealthCheck': grpc.unary_unary_rpc_method_handler(
                    servicer.HealthCheck,
                    request_deserializer=managerservice__pb2.HeartBeat.FromString,
                    response_serializer=managerservice__pb2.HeartBeat.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'managerservice.ManagerService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ManagerService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def RegisterBroker(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/managerservice.ManagerService/RegisterBroker',
            managerservice__pb2.BrokerDetails.SerializeToString,
            managerservice__pb2.Status.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def HealthCheck(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/managerservice.ManagerService/HealthCheck',
            managerservice__pb2.HeartBeat.SerializeToString,
            managerservice__pb2.HeartBeat.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)