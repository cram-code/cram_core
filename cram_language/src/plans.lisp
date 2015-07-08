;;;
;;; Copyright (c) 2009, Lorenz Moesenlechner <moesenle@cs.tum.edu>
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

(define-hook on-def-top-level-plan-hook (plan-name)
  (:documentation "Executed when a top-level-plan is defined."))

(defmacro def-top-level-cram-function (name args &body body)
  "Defines a top-level cram function. Every top-level function has its
   own episode-knowledge and task-tree.

   CAVEAT: Don't have surrounding FLET / LABLES / MACROLET /
   SYMBOL-MACROLET / LET / etc when using DEF-TOP-LEVEL-CRAM-FUNCTION
   or DEF-CRAM-FUNCTION (unless you really know what you are
   doing). They could mess with (WITH-TAGS ...) or shadow globally
   defined plans, which would not be picked up by WITH-TAGS /
   EXPAND-PLAN. See the comment before the definition of WITH-TAGS for
   more details."
  (with-gensyms (call-args)
    (multiple-value-bind (body-forms declarations doc-string)
        (parse-body body :documentation t)
      `(progn
         (eval-when (:compile-toplevel :load-toplevel :execute)
           (setf (get ',name 'plan-type) :top-level-plan)
           (setf (get ',name 'plan-lambda-list) ',args)
           (setf (get ',name 'plan-sexp) ',body)
           (on-def-top-level-plan-hook ',name))
         (defun ,name (&rest ,call-args)
           ,doc-string
           ,@declarations
           (named-top-level (:name ,name)
             (replaceable-function ,name ,args ,call-args `(top-level ,',name)
               (with-tags
                 ,@body-forms))))))))

(defmacro def-top-level-plan (name lambda-list &body body)
  (style-warn 'simple-style-warning
              :format-control "Use of deprecated form DEF-TOP-LEVEL-PLAN. Please use DEF-TOP-LEVEL-CRAM-FUNCTION instead.")
  `(def-top-level-cram-function ,name ,lambda-list ,@body))

(defmacro def-cram-function (name lambda-list &rest body)
  "Defines a cram function. All functions that should appear in the
   task-tree must be defined with def-cram-function (or def-ptr-cram-function).

   CAVEAT: See docstring of def-top-level-cram-function."
  (with-gensyms (call-args)
    (multiple-value-bind (body-forms declarations doc-string)
        (parse-body body :documentation t)
      `(progn
         (eval-when (:load-toplevel)
           (setf (get ',name 'plan-type) :plan)
           (setf (get ',name 'plan-lambda-list) ',lambda-list)
           (setf (get ',name 'plan-sexp) ',body))
         (defun ,name (&rest ,call-args)
           ,doc-string
           ,@declarations
           (replaceable-function ,name ,lambda-list ,call-args (list ',name)
             (with-tags
               ,@body-forms)))))))

(defmacro def-ptr-cram-function (name lambda-list &rest body)
  "Defines a cram function. All functions that should appear in the
   task-tree must be defined with def-cram-function (or def-ptr-cram-function).

   CAVEAT: See docstring of def-top-level-cram-function.

   Difference to def-cram-function: MUST have at least one argument in the lambda
   list. First argument in lambda list is extracted and passed as ptr-parameter.

   When a ptr-cram-function is first called (there is no corresponding task tree
   node) then the value of the ptr-parameter slot in the newly created node is
   set to the value of the first parameter.

   Note that the function defined in &body sees a lambda list from which the
   ptr-parameter has been removed. As a result, functions that are compatible
   to replace a ptr-cram-function have lambda lists that are one shorter than
   the ptr-cram-function. Example:

   (def-ptr-cram-function example-ptr (ptr-param X Y Z) &body)

   (defun compatible-ptr-replacement (X Y Z) &body)

   However, when called inside the plan, supply the ptr-param like so:

   (...
     (example-ptr \"A value\" X Y Z)
    ...)

   Since ptr-parameter is not actually passed as a parameter to the forms in
   &body, it needs to be accessible in another way. This is done by:

   (cpl-impl:get-ptr-parameter)"
  (with-gensyms (call-args)
    (multiple-value-bind (body-forms declarations doc-string)
        (parse-body body :documentation t)
      (let* ((lambda-list-cdr (cdr lambda-list)))
        `(progn
           (eval-when (:load-toplevel)
             (setf (get ',name 'plan-type) :plan)
             (setf (get ',name 'plan-lambda-list) ',lambda-list-cdr)
             (setf (get ',name 'plan-sexp) ',body))
           (defun ,name (&rest ,call-args)
             ,doc-string
             ,@declarations
             (let* ((inner-call-args (cdr ,call-args))
                    (ptr-parameter (car ,call-args)))
               (replaceable-ptr-function ,name ,lambda-list-cdr inner-call-args (list ',name) ptr-parameter
                 (with-tags
                   ,@body-forms)))))))))

(defmacro def-plan (name lambda-list &rest body)
  (style-warn 'simple-style-warning
              :format-control "Use of deprecated form DEF-PLAN. Please use DEF-CRAM-FUNCTION instead.")
  `(def-cram-function ,name ,lambda-list ,@body))
