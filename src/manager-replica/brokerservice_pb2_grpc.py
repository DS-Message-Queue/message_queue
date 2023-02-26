# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import brokerservice_pb2 as brokerservice__pb2


class BrokerServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SendTransaction = channel.unary_unary(
                '/brokerservice.BrokerService/SendTransaction',
                request_serializer=brokerservice__pb2.Transaction.SerializeToString,
                response_deserializer=brokerservice__pb2.Response.FromString,
                )
        self.GetUpdates = channel.unary_unary(
                '/brokerservice.BrokerService/GetUpdates',
                request_serializer=brokerservice__pb2.Request.SerializeToString,
                response_deserializer=brokerservice__pb2.Queries.FromString,
                )
        self.ResetBroker = channel.unary_unary(
                '/brokerservice.BrokerService/ResetBroker',
                request_serializer=brokerservice__pb2.BrokerDetails.SerializeToString,
                response_deserializer=brokerservice__pb2.Status.FromString,
                )


class BrokerServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def SendTransaction(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetUpdates(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ResetBroker(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_BrokerServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'SendTransaction': grpc.unary_unary_rpc_method_handler(
                    servicer.SendTransaction,
                    request_deserializer=brokerservice__pb2.Transaction.FromString,
                    response_serializer=brokerservice__pb2.Response.SerializeToString,
            ),
            'GetUpdates': grpc.unary_unary_rpc_method_handler(
                    servicer.GetUpdates,
                    request_deserializer=brokerservice__pb2.Request.FromString,
                    response_serializer=brokerservice__pb2.Queries.SerializeToString,
            ),
            'ResetBroker': grpc.unary_unary_rpc_method_handler(
                    servicer.ResetBroker,
                    request_deserializer=brokerservice__pb2.BrokerDetails.FromString,
                    response_serializer=brokerservice__pb2.Status.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'brokerservice.BrokerService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class BrokerService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def SendTransaction(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/brokerservice.BrokerService/SendTransaction',
            brokerservice__pb2.Transaction.SerializeToString,
            brokerservice__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetUpdates(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/brokerservice.BrokerService/GetUpdates',
            brokerservice__pb2.Request.SerializeToString,
            brokerservice__pb2.Queries.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ResetBroker(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/brokerservice.BrokerService/ResetBroker',
            brokerservice__pb2.BrokerDetails.SerializeToString,
            brokerservice__pb2.Status.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
