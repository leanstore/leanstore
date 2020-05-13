/* Save current context and install the given one.
   Copyright (C) 2002-2020 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   Contributed by Andreas Jaeger <aj@suse.de>, 2002.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, see
   <https://www.gnu.org/licenses/>.  */

#define rTMP	r16
#define rPOS	r16
#define rCPOS	r17
#define rNAT	r18

#define rB5	r18
#define rB4	r19
#define rB3	r20
#define rB2	r21
#define rB1	r22
#define rB0	r23
#define rRSC	r24
#define rBSP	r25
#define rRNAT	r26
#define rUNAT	r27
#define rFPSR	r28
#define rPFS	r29
#define rLC	r30
                                        #define rPR	r31

swapcontext:
        /* Save the preserved registers, the registers used for passing args,
           and the return address.  */
        movq        %rbx, oRBX(%rdi)
        movq        %rbp, oRBP(%rdi)
        movq        %r12, oR12(%rdi)
        movq        %r13, oR13(%rdi)
        movq        %r14, oR14(%rdi)
        movq        %r15, oR15(%rdi)

        movq        %rdi, oRDI(%rdi)
        movq        %rsi, oRSI(%rdi)
        movq        %rdx, oRDX(%rdi)
        movq        %rcx, oRCX(%rdi)
        movq        %r8, oR8(%rdi)
        movq        %r9, oR9(%rdi)

        movq        (%rsp), %rcx
        movq        %rcx, oRIP(%rdi)
        leaq        8(%rsp), %rcx                /* Exclude the return address.  */
        movq        %rcx, oRSP(%rdi)

        /* We have separate floating-point register content memory on the
           stack.  We use the __fpregs_mem block in the context.  Set the
           links up correctly.  */
        leaq        oFPREGSMEM(%rdi), %rcx
        movq        %rcx, oFPREGS(%rdi)
        /* Save the floating-point environment.  */
        fnstenv        (%rcx)
        stmxcsr oMXCSR(%rdi)

        /* The syscall destroys some registers, save them.  */
        movq        %rsi, %r12

        /* Save the current signal mask and install the new one with
           rt_sigprocmask (SIG_BLOCK, newset, oldset,_NSIG/8).  */
        leaq        oSIGMASK(%rdi), %rdx
        leaq        oSIGMASK(%rsi), %rsi
        movl        $SIG_SETMASK, %edi
        movl        $_NSIG8,%r10d
        movl        $__NR_rt_sigprocmask, %eax
        syscall
        cmpq        $-4095, %rax                /* Check %rax for error.  */
        jae        SYSCALL_ERROR_LABEL        /* Jump to error handler if error.  */

        /* Restore destroyed registers.  */
        movq        %r12, %rsi

        /* Restore the floating-point context.  Not the registers, only the
           rest.  */
        movq        oFPREGS(%rsi), %rcx
        fldenv        (%rcx)
        ldmxcsr oMXCSR(%rsi)

        /* Load the new stack pointer and the preserved registers.  */
        movq        oRSP(%rsi), %rsp
        movq        oRBX(%rsi), %rbx
        movq        oRBP(%rsi), %rbp
        movq        oR12(%rsi), %r12
        movq        oR13(%rsi), %r13
        movq        oR14(%rsi), %r14
        movq        oR15(%rsi), %r15

        /* The following ret should return to the address set with
        getcontext.  Therefore push the address on the stack.  */
        movq        oRIP(%rsi), %rcx
        pushq        %rcx

        /* Setup registers used for passing args.  */
        movq        oRDI(%rsi), %rdi
        movq        oRDX(%rsi), %rdx
        movq        oRCX(%rsi), %rcx
        movq        oR8(%rsi), %r8
        movq        oR9(%rsi), %r9

        /* Setup finally  %rsi.  */
        movq        oRSI(%rsi), %rsi

        /* Clear rax to indicate success.  */
        xorl        %eax, %eax
        ret
