package cmd

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *ExtentInfo) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "Physical":
			z.Physical, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "Physical")
				return
			}
		case "Length":
			z.Length, err = dc.ReadInt32()
			if err != nil {
				err = msgp.WrapError(err, "Length")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z ExtentInfo) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "Physical"
	err = en.Append(0x82, 0xa8, 0x50, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.Physical)
	if err != nil {
		err = msgp.WrapError(err, "Physical")
		return
	}
	// write "Length"
	err = en.Append(0xa6, 0x4c, 0x65, 0x6e, 0x67, 0x74, 0x68)
	if err != nil {
		return
	}
	err = en.WriteInt32(z.Length)
	if err != nil {
		err = msgp.WrapError(err, "Length")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z ExtentInfo) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "Physical"
	o = append(o, 0x82, 0xa8, 0x50, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c)
	o = msgp.AppendInt64(o, z.Physical)
	// string "Length"
	o = append(o, 0xa6, 0x4c, 0x65, 0x6e, 0x67, 0x74, 0x68)
	o = msgp.AppendInt32(o, z.Length)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ExtentInfo) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "Physical":
			z.Physical, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Physical")
				return
			}
		case "Length":
			z.Length, bts, err = msgp.ReadInt32Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Length")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z ExtentInfo) Msgsize() (s int) {
	s = 1 + 9 + msgp.Int64Size + 7 + msgp.Int32Size
	return
}