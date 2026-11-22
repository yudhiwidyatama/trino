/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.mysqlprotocol.protocol;

import io.netty.buffer.ByteBuf;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Little-endian and length-encoded wire primitives for the MySQL protocol.
 */
public final class MysqlPacketBuffer
{
    public static final int NULL_INDICATOR = 0xFB;

    private MysqlPacketBuffer() {}

    public static int readInt1(ByteBuf buf)
    {
        return buf.readUnsignedByte();
    }

    public static int readInt2(ByteBuf buf)
    {
        return buf.readUnsignedShortLE();
    }

    public static int readInt3(ByteBuf buf)
    {
        return buf.readUnsignedMediumLE();
    }

    public static int readInt4(ByteBuf buf)
    {
        return buf.readIntLE();
    }

    public static long readInt8(ByteBuf buf)
    {
        return buf.readLongLE();
    }

    public static long readLenEncInt(ByteBuf buf)
    {
        int first = readInt1(buf);
        if (first < 0xFB) {
            return first;
        }
        return switch (first) {
            case 0xFC -> readInt2(buf);
            case 0xFD -> readInt3(buf);
            case 0xFE -> readInt8(buf);
            default -> throw new IllegalArgumentException("Unexpected length-encoded integer marker: 0x" + Integer.toHexString(first));
        };
    }

    public static byte[] readLenEncBytes(ByteBuf buf)
    {
        long length = readLenEncInt(buf);
        if (length < 0 || length > buf.readableBytes()) {
            throw new IllegalArgumentException("Length-encoded value length " + length + " exceeds remaining bytes " + buf.readableBytes());
        }
        byte[] bytes = new byte[(int) length];
        buf.readBytes(bytes);
        return bytes;
    }

    public static String readLenEncString(ByteBuf buf)
    {
        return new String(readLenEncBytes(buf), UTF_8);
    }

    public static String readNulTerminatedString(ByteBuf buf)
    {
        int nulIndex = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) 0);
        if (nulIndex < 0) {
            return readEofString(buf);
        }
        int length = nulIndex - buf.readerIndex();
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        buf.skipBytes(1); // consume NUL terminator
        return new String(bytes, UTF_8);
    }

    public static byte[] readNulTerminatedBytes(ByteBuf buf)
    {
        int nulIndex = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) 0);
        if (nulIndex < 0) {
            byte[] remaining = new byte[buf.readableBytes()];
            buf.readBytes(remaining);
            return remaining;
        }
        int length = nulIndex - buf.readerIndex();
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        buf.skipBytes(1);
        return bytes;
    }

    public static String readEofString(ByteBuf buf)
    {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return new String(bytes, UTF_8);
    }

    public static void writeInt1(ByteBuf buf, int value)
    {
        buf.writeByte(value & 0xFF);
    }

    public static void writeInt2(ByteBuf buf, int value)
    {
        buf.writeShortLE(value & 0xFFFF);
    }

    public static void writeInt3(ByteBuf buf, int value)
    {
        buf.writeMediumLE(value & 0xFFFFFF);
    }

    public static void writeInt4(ByteBuf buf, int value)
    {
        buf.writeIntLE(value);
    }

    public static void writeInt8(ByteBuf buf, long value)
    {
        buf.writeLongLE(value);
    }

    public static void writeNull(ByteBuf buf)
    {
        buf.writeByte(NULL_INDICATOR);
    }

    public static void writeLenEncInt(ByteBuf buf, long value)
    {
        checkArgument(value >= 0, "Length-encoded integer must be non-negative: %s", value);
        if (value < 251) {
            buf.writeByte((int) value);
        }
        else if (value < 0x10000L) {
            buf.writeByte(0xFC);
            buf.writeShortLE((int) value);
        }
        else if (value < 0x1000000L) {
            buf.writeByte(0xFD);
            buf.writeMediumLE((int) value);
        }
        else {
            buf.writeByte(0xFE);
            buf.writeLongLE(value);
        }
    }

    public static void writeLenEncBytes(ByteBuf buf, byte[] bytes)
    {
        writeLenEncInt(buf, bytes.length);
        buf.writeBytes(bytes);
    }

    public static void writeLenEncString(ByteBuf buf, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        writeLenEncInt(buf, bytes.length);
        buf.writeBytes(bytes);
    }

    public static void writeNulTerminatedString(ByteBuf buf, String value)
    {
        buf.writeBytes(value.getBytes(UTF_8));
        buf.writeByte(0);
    }

    public static void writeEofString(ByteBuf buf, String value)
    {
        buf.writeBytes(value.getBytes(UTF_8));
    }
}
