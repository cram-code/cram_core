;;;
;;; Copyright (c) 2015, Mihai Pomarlan <mpomarlan@yahoo.co.uk>
;;; All rights reserved.
;;;
;;; Redistribution and use in source and binary forms, with or without
;;; modification, are permitted provided that the following conditions are met:
;;;
;;;     * Redistributions of source code must retain the above copyright
;;;       notice, this list of conditions and the following disclaimer.
;;;     * Redistributions in binary form must reproduce the above copyright
;;;       notice, this list of conditions and the following disclaimer in the
;;;       documentation and/or other materials provided with the distribution.
;;;     * Neither the name of Willow Garage, Inc. nor the names of its
;;;       contributors may be used to endorse or promote products derived from
;;;       this software without specific prior written permission.
;;;
;;; THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
;;; AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
;;; IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
;;; ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
;;; LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
;;; CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
;;; SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
;;; INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
;;; CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
;;; ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
;;; POSSIBILITY OF SUCH DAMAGE.
;;;


(in-package :cpl-impl)

;;; This file defines function versions of some of the macros that make up the plan language (seq, par, try-in-order etc.)
;;;
;;; The reason is that these ptr functions show up in the task tree. In particular, their ptr-argument is changeable by
;;; plan transformation, which means the plan could decide, at runtime, what to put inside a sequence or parallel block.

(def-ptr-cram-function seq-ptr (ptr-parameter)
"Run function objects from ptr-parameter in sequence. Returns the return value of the last function object.

PTR-PARAMETER must be a list of function objects. Example:

(list (lambda () (some-function arg-1 arg-2...)) (lambda () ...))

Function objects should wrap functions defined with def-[ptr-]cram-function if they are to show up in the task tree.

Will fail as soon as one of the function objects produces a failure.

TODO: right now all these anonymous lambdas will mess up de/serialization. Will need some way to detect whether
ptr-parameter slots in the task tree are safely serializable and/or provide a serialization mechanism for some
reasonable class of function objects/closures."
  (car (last (mapcar #'funcall ptr-parameter))))


