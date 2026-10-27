/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.evm.v2;

import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_void;
import static java.lang.constant.ConstantDescs.INIT_NAME;
import static java.lang.constant.ConstantDescs.MTD_void;

import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.JumpDestOperation;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.v2.operation.AddModOperationV2;
import org.hyperledger.besu.evm.v2.operation.AddOperationV2;
import org.hyperledger.besu.evm.v2.operation.AndOperationV2;
import org.hyperledger.besu.evm.v2.operation.ByteOperationV2;
import org.hyperledger.besu.evm.v2.operation.CountLeadingZerosOperationV2;
import org.hyperledger.besu.evm.v2.operation.DivOperationV2;
import org.hyperledger.besu.evm.v2.operation.DupOperationV2;
import org.hyperledger.besu.evm.v2.operation.EqOperationV2;
import org.hyperledger.besu.evm.v2.operation.GtOperationV2;
import org.hyperledger.besu.evm.v2.operation.IsZeroOperationV2;
import org.hyperledger.besu.evm.v2.operation.LtOperationV2;
import org.hyperledger.besu.evm.v2.operation.ModOperationV2;
import org.hyperledger.besu.evm.v2.operation.MulModOperationV2;
import org.hyperledger.besu.evm.v2.operation.MulOperationV2;
import org.hyperledger.besu.evm.v2.operation.NotOperationV2;
import org.hyperledger.besu.evm.v2.operation.OrOperationV2;
import org.hyperledger.besu.evm.v2.operation.PopOperationV2;
import org.hyperledger.besu.evm.v2.operation.PushOperationV2;
import org.hyperledger.besu.evm.v2.operation.SDivOperationV2;
import org.hyperledger.besu.evm.v2.operation.SGtOperationV2;
import org.hyperledger.besu.evm.v2.operation.SLtOperationV2;
import org.hyperledger.besu.evm.v2.operation.SModOperationV2;
import org.hyperledger.besu.evm.v2.operation.SarOperationV2;
import org.hyperledger.besu.evm.v2.operation.ShlOperationV2;
import org.hyperledger.besu.evm.v2.operation.ShrOperationV2;
import org.hyperledger.besu.evm.v2.operation.SignExtendOperationV2;
import org.hyperledger.besu.evm.v2.operation.SubOperationV2;
import org.hyperledger.besu.evm.v2.operation.SwapOperationV2;
import org.hyperledger.besu.evm.v2.operation.XorOperationV2;

import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassHierarchyResolver;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Opcode;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HexFormat;

/**
 * Generates the class of an {@link OperationGroup}: a method that calls the static version of each
 * operation in turn, with no switch between them, which C2 then compiles as one piece of code.
 */
final class OperationGroupCompiler {

  private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

  private static final ClassFile CLASS_FILE =
      ClassFile.of(
          ClassFile.ClassHierarchyResolverOption.of(
              ClassHierarchyResolver.defaultResolver()
                  .orElse(
                      ClassHierarchyResolver.ofClassLoading(
                          OperationGroupCompiler.class.getClassLoader()))));

  private static final ClassDesc CD_GENERATED =
      ClassDesc.of(OperationGroup.class.getPackageName(), "GeneratedOperationGroup");
  private static final ClassDesc CD_GROUP = desc(OperationGroup.class);
  private static final ClassDesc CD_FRAME = desc(MessageFrame.class);
  private static final ClassDesc CD_RESULT = desc(OperationResult.class);

  private static final MethodTypeDesc MTD_RUN =
      MethodTypeDesc.of(CD_RESULT, CD_FRAME, desc(byte[].class), CD_int);
  private static final MethodTypeDesc MTD_FRAME_OPERATION = MethodTypeDesc.of(CD_RESULT, CD_FRAME);
  private static final MethodTypeDesc MTD_INDEX_OPERATION =
      MethodTypeDesc.of(CD_RESULT, CD_FRAME, CD_int);
  private static final MethodTypeDesc MTD_PUSH_OPERATION =
      MethodTypeDesc.of(CD_RESULT, CD_FRAME, desc(byte[].class), CD_int, CD_int);
  private static final MethodTypeDesc MTD_HALT_REASON =
      MethodTypeDesc.of(desc(ExceptionalHaltReason.class));
  private static final MethodTypeDesc MTD_SET_PC = MethodTypeDesc.of(CD_void, CD_int);

  // the locals of run
  private static final int FRAME = 1;
  private static final int CODE = 2;
  private static final int PC = 3;
  private static final int RESULT = 4;

  private OperationGroupCompiler() {}

  /**
   * Generates the group of a sequence of operations.
   *
   * @param opcodes the opcodes, each one of {@link OperationGroups#isGroupOpcode}
   * @return the group
   */
  static OperationGroup compile(final byte[] opcodes) {
    final byte[] bytes =
        CLASS_FILE.build(
            CD_GENERATED,
            clazz ->
                clazz
                    .withFlags(ClassFile.ACC_PUBLIC | ClassFile.ACC_FINAL | ClassFile.ACC_SUPER)
                    .withSuperclass(CD_GROUP)
                    .withMethodBody(
                        INIT_NAME,
                        MTD_void,
                        ClassFile.ACC_PUBLIC,
                        code ->
                            code.aload(0).invokespecial(CD_GROUP, INIT_NAME, MTD_void).return_())
                    .withMethodBody(
                        "run",
                        MTD_RUN,
                        ClassFile.ACC_PUBLIC | ClassFile.ACC_FINAL,
                        code -> run(code, opcodes)));
    try {
      final MethodHandles.Lookup generated = LOOKUP.defineHiddenClass(bytes, true);
      return (OperationGroup)
          generated
              .findConstructor(generated.lookupClass(), MethodType.methodType(void.class))
              .invoke();
    } catch (final Throwable t) {
      throw new IllegalStateException(
          "Cannot define the operation group of opcodes " + HexFormat.of().formatHex(opcodes), t);
    }
  }

  private static void run(final CodeBuilder code, final byte[] opcodes) {
    int offset = 0;
    for (final byte b : opcodes) {
      final int opcode = b & 0xff;
      if (opcode != JumpDestOperation.OPCODE) {
        operation(code, opcode, offset);
        final int haltOffset = offset;
        code.astore(RESULT)
            .aload(RESULT)
            .invokevirtual(CD_RESULT, "getHaltReason", MTD_HALT_REASON)
            .ifThen(Opcode.IFNONNULL, halt -> setPc(halt, haltOffset).aload(RESULT).areturn());
      }
      offset += OperationGroups.size(opcode);
    }
    setPc(code, offset).aconst_null().areturn();
  }

  private static CodeBuilder setPc(final CodeBuilder code, final int offset) {
    return code.aload(FRAME)
        .iload(PC)
        .loadConstant(offset)
        .iadd()
        .invokevirtual(CD_FRAME, "setPC", MTD_SET_PC);
  }

  /** Calls the static version of an operation, leaving its result on the stack. */
  private static void operation(final CodeBuilder code, final int opcode, final int offset) {
    if (opcode >= PushOperationV2.PUSH_BASE && opcode <= 0x7f) {
      final Class<?> push =
          opcode <= 0x60
              ? PushOperationV2.SingleByte.class
              : opcode <= 0x67 ? PushOperationV2.SingleLimb.class : PushOperationV2.MultiLimb.class;
      code.aload(FRAME)
          .aload(CODE)
          .iload(PC)
          .loadConstant(offset)
          .iadd()
          .loadConstant(opcode - PushOperationV2.PUSH_BASE)
          .invokestatic(desc(push), "staticOperation", MTD_PUSH_OPERATION);
    } else if (opcode > DupOperationV2.DUP_BASE && opcode <= 0x8f) {
      code.aload(FRAME)
          .loadConstant(opcode - DupOperationV2.DUP_BASE)
          .invokestatic(desc(DupOperationV2.class), "staticOperation", MTD_INDEX_OPERATION);
    } else if (opcode > SwapOperationV2.SWAP_BASE && opcode <= 0x9f) {
      code.aload(FRAME)
          .loadConstant(opcode - SwapOperationV2.SWAP_BASE)
          .invokestatic(desc(SwapOperationV2.class), "staticOperation", MTD_INDEX_OPERATION);
    } else {
      code.aload(FRAME)
          .invokestatic(desc(frameOperation(opcode)), "staticOperation", MTD_FRAME_OPERATION);
    }
  }

  /** The class of an operation whose static version takes only the frame. */
  private static Class<?> frameOperation(final int opcode) {
    return switch (opcode) {
      case 0x01 -> AddOperationV2.class;
      case 0x02 -> MulOperationV2.class;
      case 0x03 -> SubOperationV2.class;
      case 0x04 -> DivOperationV2.class;
      case 0x05 -> SDivOperationV2.class;
      case 0x06 -> ModOperationV2.class;
      case 0x07 -> SModOperationV2.class;
      case 0x08 -> AddModOperationV2.class;
      case 0x09 -> MulModOperationV2.class;
      case 0x0b -> SignExtendOperationV2.class;
      case 0x10 -> LtOperationV2.class;
      case 0x11 -> GtOperationV2.class;
      case 0x12 -> SLtOperationV2.class;
      case 0x13 -> SGtOperationV2.class;
      case 0x14 -> EqOperationV2.class;
      case 0x15 -> IsZeroOperationV2.class;
      case 0x16 -> AndOperationV2.class;
      case 0x17 -> OrOperationV2.class;
      case 0x18 -> XorOperationV2.class;
      case 0x19 -> NotOperationV2.class;
      case 0x1a -> ByteOperationV2.class;
      case 0x1b -> ShlOperationV2.class;
      case 0x1c -> ShrOperationV2.class;
      case 0x1d -> SarOperationV2.class;
      case 0x1e -> CountLeadingZerosOperationV2.class;
      case 0x50 -> PopOperationV2.class;
      default -> throw new IllegalArgumentException("Opcode " + opcode + " is not in groups");
    };
  }

  private static ClassDesc desc(final Class<?> type) {
    return type.describeConstable().orElseThrow();
  }
}
