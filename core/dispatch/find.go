// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package dispatch

import (
	"context"

	"time"

	"errors"
	"github.com/mongodb/mongo-go-driver/bson/bsoncodec"
	"github.com/mongodb/mongo-go-driver/core/command"
	"github.com/mongodb/mongo-go-driver/core/connection"
	"github.com/mongodb/mongo-go-driver/core/description"
	"github.com/mongodb/mongo-go-driver/core/session"
	"github.com/mongodb/mongo-go-driver/core/topology"
	"github.com/mongodb/mongo-go-driver/core/uuid"
	"github.com/mongodb/mongo-go-driver/core/wiremessage"
	"github.com/mongodb/mongo-go-driver/options"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
)

// Find handles the full cycle dispatch and execution of a find command against the provided
// topology.
func Find(
	ctx context.Context,
	cmd command.Find,
	topo *topology.Topology,
	selector description.ServerSelector,
	clientID uuid.UUID,
	pool *session.Pool,
	registry *bsoncodec.Registry,
	opts ...*options.FindOptions,
) (command.Cursor, error) {

	ss, err := topo.SelectServer(ctx, selector)
	if err != nil {
		return nil, err
	}

	desc := ss.Description()
	conn, err := ss.Connection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if desc.WireVersion.Max < 4 {
		return legacyFind(ctx, cmd, registry, ss, desc, conn, opts...)
	}

	rp, err := getReadPrefBasedOnTransaction(cmd.ReadPref, cmd.Session)
	if err != nil {
		return nil, err
	}
	cmd.ReadPref = rp

	// If no explicit session and deployment supports sessions, start implicit session.
	if cmd.Session == nil && topo.SupportsSessions() {
		cmd.Session, err = session.NewClientSession(pool, clientID, session.Implicit)
		if err != nil {
			return nil, err
		}
	}

	fo := options.MergeFindOptions(opts...)
	if fo.AllowPartialResults != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"allowPartialResults", bsonx.Boolean(*fo.AllowPartialResults)})
	}
	if fo.BatchSize != nil {
		elem := bsonx.Elem{"batchSize", bsonx.Int32(*fo.BatchSize)}
		cmd.Opts = append(cmd.Opts, elem)
		cmd.CursorOpts = append(cmd.CursorOpts, elem)

		if fo.Limit != nil && *fo.BatchSize != 0 && *fo.Limit <= int64(*fo.BatchSize) {
			cmd.Opts = append(cmd.Opts, bsonx.Elem{"singleBatch", bsonx.Boolean(true)})
		}
	}
	if fo.Collation != nil {
		if desc.WireVersion.Max < 5 {
			return nil, ErrCollation
		}
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"collation", bsonx.Document(fo.Collation.ToDocument())})
	}
	if fo.Comment != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"comment", bsonx.String(*fo.Comment)})
	}
	if fo.CursorType != nil {
		switch *fo.CursorType {
		case options.Tailable:
			cmd.Opts = append(cmd.Opts, bsonx.Elem{"tailable", bsonx.Boolean(true)})
		case options.TailableAwait:
			cmd.Opts = append(cmd.Opts, bsonx.Elem{"tailable", bsonx.Boolean(true)}, bsonx.Elem{"awaitData", bsonx.Boolean(true)})
		}
	}
	if fo.Hint != nil {
		hintElem, err := interfaceToElement("hint", fo.Hint, registry)
		if err != nil {
			return nil, err
		}

		cmd.Opts = append(cmd.Opts, hintElem)
	}
	if fo.Limit != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"limit", bsonx.Int64(*fo.Limit)})
	}
	if fo.Max != nil {
		maxElem, err := interfaceToElement("max", fo.Max, registry)
		if err != nil {
			return nil, err
		}

		cmd.Opts = append(cmd.Opts, maxElem)
	}
	if fo.MaxAwaitTime != nil {
		// Specified as maxTimeMS on the in the getMore command and not given in initial find command.
		cmd.CursorOpts = append(cmd.CursorOpts, bsonx.Elem{"maxTimeMS", bsonx.Int64(int64(*fo.MaxAwaitTime / time.Millisecond))})
	}
	if fo.MaxTime != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"maxTimeMS", bsonx.Int64(int64(*fo.MaxTime / time.Millisecond))})
	}
	if fo.Min != nil {
		minElem, err := interfaceToElement("min", fo.Min, registry)
		if err != nil {
			return nil, err
		}

		cmd.Opts = append(cmd.Opts, minElem)
	}
	if fo.NoCursorTimeout != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"noCursorTimeout", bsonx.Boolean(*fo.NoCursorTimeout)})
	}
	if fo.OplogReplay != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"oplogReplay", bsonx.Boolean(*fo.OplogReplay)})
	}
	if fo.Projection != nil {
		projElem, err := interfaceToElement("projection", fo.Projection, registry)
		if err != nil {
			return nil, err
		}

		cmd.Opts = append(cmd.Opts, projElem)
	}
	if fo.ReturnKey != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"returnKey", bsonx.Boolean(*fo.ReturnKey)})
	}
	if fo.ShowRecordID != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"showRecordId", bsonx.Boolean(*fo.ShowRecordID)})
	}
	if fo.Skip != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"skip", bsonx.Int64(*fo.Skip)})
	}
	if fo.Snapshot != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"snapshot", bsonx.Boolean(*fo.Snapshot)})
	}
	if fo.Sort != nil {
		sortElem, err := interfaceToElement("sort", fo.Sort, registry)
		if err != nil {
			return nil, err
		}

		cmd.Opts = append(cmd.Opts, sortElem)
	}

	return cmd.RoundTrip(ctx, desc, ss, conn)
}

// legacyFind handles the dispatch and execution of a find operation against a pre-3.2 server.
func legacyFind(
	ctx context.Context,
	cmd command.Find,
	registry *bsoncodec.Registry,
	ss *topology.SelectedServer,
	desc description.SelectedServer,
	conn connection.Connection,
	opts ...*options.FindOptions,
) (command.Cursor, error) {
	// TODO slaveok and read pref
	// TODO batch size of 1 (in doc)

	query := wiremessage.Query{
		FullCollectionName: cmd.NS.DB + "." + cmd.NS.Collection,
	}
	fo := options.MergeFindOptions(opts...)

	err := appendLegacyOptions(&cmd, &query, fo, registry)
	if err != nil {
		return nil, err
	}

	var queryDoc bsonx.Doc
	queryDoc = append(queryDoc, cmd.Opts...)

	// filter must be wrapped in $query if other $modifiers are used
	if cmd.Filter != nil {
		if len(cmd.Opts) > 0 {
			queryDoc = queryDoc.Append("$query", bsonx.Document(cmd.Filter))
		} else {
			queryDoc = cmd.Filter
		}
	}

	queryRaw, err := queryDoc.MarshalBSON()
	if err != nil {
		return nil, err
	}
	query.Query = queryRaw

	err = conn.WriteWireMessage(ctx, query)
	if err != nil {
		if _, ok := err.(command.Error); ok {
			return nil, err
		}
		// TODO probably shouldn't have TransientTransactionError?
		return nil, command.Error{
			Message: err.Error(),
			Labels:  []string{command.TransientTransactionError, command.NetworkError},
		}
	}

	wm, err := conn.ReadWireMessage(ctx)
	if err != nil {
		if _, ok := err.(command.Error); ok {
			return nil, err
		}
		// Connection errors are transient
		return nil, command.Error{
			Message: err.Error(),
			Labels:  []string{command.TransientTransactionError, command.NetworkError},
		}
	}

	reply, ok := wm.(wiremessage.Reply)
	if !ok {
		return nil, errors.New("did not receive OP_REPLY response")
	}

	err = validateOpReply(reply)
	if err != nil {
		return nil, err
	}

	var cursorLimit int32
	var cursorBatchSize int32
	if fo.Limit != nil {
		cursorLimit = int32(*fo.Limit)
	}
	if fo.BatchSize != nil {
		cursorBatchSize = int32(*fo.BatchSize)
	}

	c, err := ss.BuildLegacyCursor(cmd.NS, reply.CursorID, reply.Documents, cursorLimit, cursorBatchSize)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func validateOpReply(reply wiremessage.Reply) error {
	if int(reply.NumberReturned) != len(reply.Documents) {
		return command.NewCommandResponseError("malformed OP_REPLY: NumberReturned does not match number of documents returned", nil)
	}

	if reply.ResponseFlags&wiremessage.QueryFailure > 0 {
		return command.QueryFailureError{
			Message:  "query failure",
			Response: reply.Documents[0],
		}
	}

	return nil
}

func appendLegacyOptions(cmd *command.Find, query *wiremessage.Query, fo *options.FindOptions, registry *bsoncodec.Registry) error {
	query.NumberToReturn = calculateNumberToReturn(fo)
	if fo.AllowPartialResults != nil {
		query.Flags |= wiremessage.Partial
	}
	if fo.Collation != nil {
		return ErrCollation
	}
	if fo.Comment != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"$comment", bsonx.String(*fo.Comment)})
	}
	if fo.CursorType != nil {
		switch *fo.CursorType {
		case options.Tailable:
			query.Flags |= wiremessage.TailableCursor
		case options.TailableAwait:
			query.Flags |= wiremessage.TailableCursor
			query.Flags |= wiremessage.AwaitData
		}
	}
	if fo.Hint != nil {
		hintElem, err := interfaceToElement("$hint", fo.Hint, registry)
		if err != nil {
			return err
		}

		cmd.Opts = append(cmd.Opts, hintElem)
	}
	if fo.Max != nil {
		maxElem, err := interfaceToElement("$max", fo.Max, registry)
		if err != nil {
			return err
		}

		cmd.Opts = append(cmd.Opts, maxElem)
	}
	if fo.MaxAwaitTime != nil {
		// Specified as maxTimeMS on the in the getMore command and not given in initial find command.
		cmd.CursorOpts = append(cmd.CursorOpts, bsonx.Elem{"maxTimeMS", bsonx.Int64(int64(*fo.MaxAwaitTime / time.Millisecond))})
	}
	if fo.MaxTime != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"$maxTimeMS", bsonx.Int64(int64(*fo.MaxTime / time.Millisecond))})
	}
	if fo.Min != nil {
		minElem, err := interfaceToElement("$min", fo.Min, registry)
		if err != nil {
			return err
		}

		cmd.Opts = append(cmd.Opts, minElem)
	}
	if fo.NoCursorTimeout != nil {
		query.Flags |= wiremessage.NoCursorTimeout
	}
	if fo.OplogReplay != nil {
		query.Flags |= wiremessage.OplogReplay
	}
	if fo.Projection != nil {
		projDoc, err := interfaceToDocument(fo.Projection, registry)
		if err != nil {
			return err
		}

		projRaw, err := projDoc.MarshalBSON()
		if err != nil {
			return err
		}
		query.ReturnFieldsSelector = projRaw
	}
	if fo.ReturnKey != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"$returnKey", bsonx.Boolean(*fo.ReturnKey)})
	}
	if fo.ShowRecordID != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"$showDiskLoc", bsonx.Boolean(*fo.ShowRecordID)})
	}
	if fo.Skip != nil {
		query.NumberToSkip = int32(*fo.Skip)
	}
	if fo.Snapshot != nil {
		cmd.Opts = append(cmd.Opts, bsonx.Elem{"$snapshot", bsonx.Boolean(*fo.Snapshot)})
	}
	if fo.Sort != nil {
		sortElem, err := interfaceToElement("$orderby", fo.Sort, registry)
		if err != nil {
			return err
		}

		cmd.Opts = append(cmd.Opts, sortElem)
	}

	return nil
}

// calculate the number to return for the first find query
func calculateNumberToReturn(opts *options.FindOptions) int32 {
	var numReturn int32
	var limit int32
	var batchSize int32

	if opts.Limit != nil {
		limit = int32(*opts.Limit)
	}
	if opts.BatchSize != nil {
		batchSize = int32(*opts.BatchSize)
	}

	if limit < 0 {
		numReturn = limit
	} else if limit == 0 {
		numReturn = batchSize
	} else if limit < batchSize {
		numReturn = limit
	} else {
		numReturn = batchSize
	}

	return numReturn
}
