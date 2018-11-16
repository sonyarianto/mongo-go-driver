// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package wiremessage

import (
	"errors"
	"strings"
	"fmt"
)

// GetMore represents the OP_GET_MORE message of the MongoDB wire protocol.
type GetMore struct {
	MsgHeader          Header
	FullCollectionName string
	NumberToReturn     int32
	CursorID           int64
}

// MarshalWireMessage implements the Marshaler and WireMessage interfaces.
func (gm GetMore) MarshalWireMessage() ([]byte, error) {
	b := make([]byte, 0, gm.Len())
	return gm.AppendWireMessage(b)
}

// ValidateWireMessage implements the Validator and WireMessage interfaces.
func (gm GetMore) ValidateWireMessage() error {
	if int(gm.MsgHeader.MessageLength) != gm.Len() {
		return errors.New("incorrect header: message length is not correct")
	}
	if gm.MsgHeader.OpCode != OpGetMore {
		return errors.New("incorrect header: op code is not OpGetMore")
	}
	if strings.Index(gm.FullCollectionName, ".") == -1 {
		return errors.New("incorrect header: collection name does not contain a dot")
	}

	return nil
}

// AppendWireMessage implements the Appender and WireMessage interfaces.
//
// AppendWireMessage will set the MessageLength property of the MsgHeader
// if it is zero. It will also set the OpCode to OpGetMore if the OpCode is
// zero. If either of these properties are non-zero and not correct, this
// method will return both the []byte with the wire message appended to it
// and an invalid header error.
func (gm GetMore) AppendWireMessage(b []byte) ([]byte, error) {
	var err error
	err = gm.MsgHeader.SetDefaults(gm.Len(), OpGetMore)

	b = gm.MsgHeader.AppendHeader(b)
	b = appendCString(b, gm.FullCollectionName)
	b = appendInt32(b, gm.NumberToReturn)
	b = appendInt64(b, gm.CursorID)
	return b, err
}

// String implements the fmt.Stringer interface.
func (gm GetMore) String() string {
	return fmt.Sprintf(
		`OP_GETMORE{MsgHeader: %s, FullCollectionName: %s, NumberToReturn: %d, CursorID: %d}`,
		gm.MsgHeader, gm.FullCollectionName, gm.NumberToReturn, gm.CursorID,
	)
}

// Len implements the WireMessage interface.
func (gm GetMore) Len() int {
	// Header + CollectionName + Null Terminator + Return + CursorID
	return 16 + len(gm.FullCollectionName) + 1 + 4 + 8
}

// UnmarshalWireMessage implements the Unmarshaler interface.
func (gm *GetMore) UnmarshalWireMessage([]byte) error {
	panic("not implemented")
}
