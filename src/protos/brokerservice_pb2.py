# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: src/protos/brokerservice.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1esrc/protos/brokerservice.proto\x12\rbrokerservice\"\x18\n\x08Response\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\"\x1b\n\x0bTransaction\x12\x0c\n\x04\x64\x61ta\x18\x01 \x01(\x0c\"+\n\x07Request\x12\r\n\x05topic\x18\x01 \x01(\t\x12\x11\n\tpartition\x18\x02 \x01(\t\"\x08\n\x06Status\"\x16\n\x05Query\x12\r\n\x05query\x18\x01 \x01(\t\"!\n\rBrokerDetails\x12\x10\n\x08\x62rokerId\x18\x01 \x01(\x05\x32\xdf\x01\n\rBrokerService\x12H\n\x0fSendTransaction\x12\x1a.brokerservice.Transaction\x1a\x17.brokerservice.Response\"\x00\x12>\n\nGetUpdates\x12\x16.brokerservice.Request\x1a\x14.brokerservice.Query\"\x00\x30\x01\x12\x44\n\x0bResetBroker\x12\x1c.brokerservice.BrokerDetails\x1a\x15.brokerservice.Status\"\x00\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'src.protos.brokerservice_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _RESPONSE._serialized_start=49
  _RESPONSE._serialized_end=73
  _TRANSACTION._serialized_start=75
  _TRANSACTION._serialized_end=102
  _REQUEST._serialized_start=104
  _REQUEST._serialized_end=147
  _STATUS._serialized_start=149
  _STATUS._serialized_end=157
  _QUERY._serialized_start=159
  _QUERY._serialized_end=181
  _BROKERDETAILS._serialized_start=183
  _BROKERDETAILS._serialized_end=216
  _BROKERSERVICE._serialized_start=219
  _BROKERSERVICE._serialized_end=442
# @@protoc_insertion_point(module_scope)
