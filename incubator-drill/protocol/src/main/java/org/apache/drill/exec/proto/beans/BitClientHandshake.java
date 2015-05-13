/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;

public final class BitClientHandshake implements Externalizable, Message<BitClientHandshake>, Schema<BitClientHandshake>
{

    public static Schema<BitClientHandshake> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static BitClientHandshake getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final BitClientHandshake DEFAULT_INSTANCE = new BitClientHandshake();

    
    private int rpcVersion;
    private RpcChannel channel;
    private FragmentHandle handle;

    public BitClientHandshake()
    {
        
    }

    // getters and setters

    // rpcVersion

    public int getRpcVersion()
    {
        return rpcVersion;
    }

    public BitClientHandshake setRpcVersion(int rpcVersion)
    {
        this.rpcVersion = rpcVersion;
        return this;
    }

    // channel

    public RpcChannel getChannel()
    {
        return channel == null ? RpcChannel.BIT_DATA : channel;
    }

    public BitClientHandshake setChannel(RpcChannel channel)
    {
        this.channel = channel;
        return this;
    }

    // handle

    public FragmentHandle getHandle()
    {
        return handle;
    }

    public BitClientHandshake setHandle(FragmentHandle handle)
    {
        this.handle = handle;
        return this;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, this);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, this);
    }

    // message method

    public Schema<BitClientHandshake> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public BitClientHandshake newMessage()
    {
        return new BitClientHandshake();
    }

    public Class<BitClientHandshake> typeClass()
    {
        return BitClientHandshake.class;
    }

    public String messageName()
    {
        return BitClientHandshake.class.getSimpleName();
    }

    public String messageFullName()
    {
        return BitClientHandshake.class.getName();
    }

    public boolean isInitialized(BitClientHandshake message)
    {
        return true;
    }

    public void mergeFrom(Input input, BitClientHandshake message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.rpcVersion = input.readInt32();
                    break;
                case 2:
                    message.channel = RpcChannel.valueOf(input.readEnum());
                    break;
                case 3:
                    message.handle = input.mergeObject(message.handle, FragmentHandle.getSchema());
                    break;

                default:
                    input.handleUnknownField(number, this);
            }   
        }
    }


    public void writeTo(Output output, BitClientHandshake message) throws IOException
    {
        if(message.rpcVersion != 0)
            output.writeInt32(1, message.rpcVersion, false);

        if(message.channel != null)
             output.writeEnum(2, message.channel.number, false);

        if(message.handle != null)
             output.writeObject(3, message.handle, FragmentHandle.getSchema(), false);

    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "rpcVersion";
            case 2: return "channel";
            case 3: return "handle";
            default: return null;
        }
    }

    public int getFieldNumber(String name)
    {
        final Integer number = __fieldMap.get(name);
        return number == null ? 0 : number.intValue();
    }

    private static final java.util.HashMap<String,Integer> __fieldMap = new java.util.HashMap<String,Integer>();
    static
    {
        __fieldMap.put("rpcVersion", 1);
        __fieldMap.put("channel", 2);
        __fieldMap.put("handle", 3);
    }
    
}
