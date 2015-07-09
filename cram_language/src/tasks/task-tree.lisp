;;;
;;; Copyright (c) 2009, Lorenz Moesenlechner <moesenle@cs.tum.edu>,
;;;                     Nikolaus Demmel <demmeln@cs.tum.edu>
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

;;; NOTE: DOC: Paths in CRAM are read from right to left, such that when
;;; decending a task-tree or plan-tree, the path grows at the left.

;;; Represents code sexp is the sexp of the code, function is a
;;; compiled function and tasks is the list of tasks, in reverse
;;; order.
(defstruct code
  "Represents a piece of code that can be replaced. It contains the
  sexp as well as a function object that can be executed. Further, it
  contains the top-level task and all tasks in the lexical environment
  of the top-level task. This can be used to get the variable bindings
  of a specific task without the need to walk the task tree."
  sexp
  function
  task
  parameters
  ptr-parameter)

(defstruct task-tree-node
  (code nil)
  (code-replacements (list))
  (parent nil)
  (children nil)
  (path nil)
  (lock (sb-thread:make-mutex)))

;; #demmeln: ask rittweiler for advice on layout of  pretty printing

;;; Define our own pretty print method, to avoid infinit recursion
;;; when *print-circle* is nil. It is similar to the general struct
;;; pretty printer but doesn't print the parent slot. Some dynamic
;;; variables control how verbose task-trees are displayed.

(defparameter *task-tree-print-path* :full
  "Determines how task-tree-nodes are pretty-printed. One of (nil :one :full). Default :full")
(defparameter *task-tree-print-code* nil
  "Determines how task-tree-nodes are pretty-printed. Generalized Boolean. Default nil")
(defparameter *task-tree-print-children* :count
  "Determines how task-tree-nodes are pretty-printed. One of (nil :count :full). Default :count")
(defparameter *task-tree-print-identity* t
  "Determines how task-tree-nodes are pretty-printed. Generalized Boolean. Default t")

(defmethod print-object ((object task-tree-node) stream)
  (assert (member *task-tree-print-children* '(nil :count :full)))
  (assert (member *task-tree-print-path* '(nil :one :full)))
  (print-unreadable-object (object stream :type t :identity *task-tree-print-identity*)
    (when *task-tree-print-path*
      (let ((path (task-tree-node-path object)))
        (format stream "~:@_:PATH ~:@_~W" (case *task-tree-print-path*
                                            (:one  (list (first path) "..."))
                                            (:full path)
                                            (otherwise (assert nil))))))
    (when *task-tree-print-code*
      (let ((code (task-tree-node-code object)))
        (format stream "~:@_ :CODE ~:@_~W" code)))
    (when *task-tree-print-children*
      (let ((children (task-tree-node-children object)))
        (case *task-tree-print-children*
          (:count (format stream "~:@_ :CHILD-COUNT ~:@_~W" (length children)))
          (:full (format stream "~:@_ :CHILDREN ~:@_~W" children))
          (otherwise (assert nil)))))
    (format stream "~:@_")))

(define-hook cram-language::on-preparing-task-execution (name log-parameters log-pattern))
(define-hook cram-language::on-finishing-task-execution (id))

(defmacro with-task-tree-node ((&key
                                  (path-part
                                   (error "Path parameter is required."))
                                  (name "WITH-TASK-TREE-NODE")
                                  sexp lambda-list parameters ptr-parameter is-ptr-task
                                  log-parameters log-pattern)
                               &body body)
  "Executes a body under a specific path. Sexp, lambda-list, ptr-parameter and parameters are optional."
  (with-gensyms (task)
    `(let* ((*current-path* (cons ,path-part *current-path*))
            (*current-task-tree-node* (ensure-tree-node *current-path*)))
       (declare (special *current-path* *current-task-tree-node*))
       (let ((log-id (first (cram-language::on-preparing-task-execution
                             ,name ,log-parameters ,log-pattern))))
         (unwind-protect
              (join-task
               (sb-thread:with-mutex ((task-tree-node-lock
                                       *current-task-tree-node*))
                 (let ((,task (make-task
                               :name ',(gensym (format nil "[~a]-" name))
                               :sexp ',(or sexp body)
                               :ptr-parameter ,ptr-parameter
                               :is-ptr-task ,is-ptr-task
                               :function (lambda ,lambda-list
                                           ,@body)
                               :parameters ,parameters)))
                   (execute ,task)
                   ,task)))
           (cram-language::on-finishing-task-execution log-id))))))

(defmacro replaceable-function-base (name lambda-list parameters path-part ptr-parameter is-ptr-task
                                &body body)
  `(with-task-tree-node (:path-part ,path-part
                         :name ,(format nil "REPLACEABLE-FUNCTION-~a" name)
                         :sexp `(replaceable-function ,',name ,',lambda-list . ,',body)
                         :lambda-list ,lambda-list
                         :is-ptr-task ,is-ptr-task
                         :ptr-parameter ,ptr-parameter
                         :parameters ,parameters)
     ,@body))

(defmacro replaceable-function (name lambda-list parameters path-part
                                &body body)
  "Besides the replacement of simple code parts defined with 'with-task-tree-node',
   it is necessary to also pass parameters to the replaceable code
   parts. For that, replaceable functions can be defined. They are not
   real functions, i.e. they do change any symbol-function or change
   the lexical environment. 'name' is used to mark such functions in
   the code-sexp. More specifically, the sexp is built like follows:
   `(replaceable-function ,name ,lambda-list ,@body).
   The 'parameters' parameter contains the values to call the function with."
  `(replaceable-function-base ,name ,lambda-list ,parameters ,path-part nil nil ,@body))

(defmacro replaceable-ptr-function (ptr-parameter name lambda-list parameters path-part
                                &body body)
  "Besides the replacement of simple code parts defined with 'with-task-tree-node',
   it is necessary to also pass parameters to the replaceable code
   parts. For that, replaceable functions can be defined. They are not
   real functions, i.e. they do change any symbol-function or change
   the lexical environment. 'name' is used to mark such functions in
   the code-sexp. More specifically, the sexp is built like follows:
   `(replaceable-function ,name ,lambda-list ,@body).
   The 'parameters' parameter contains the values to call the function with.

   ptr-parameter: its initial value gets stored in the task tree, and thereafter 
   it is from the task tree that its value is retrieved when running the cram function. 
   This has two consequences:
   
     - it should only be used to send 'compile-time' values to the cram function OR
     - it can be used to send values tweaked by plan transformation."
  `(replaceable-function-base ,name ,lambda-list ,parameters ,path-part ,ptr-parameter T ,@body))

(defun execute-task-tree-node (node)
  (let ((code (task-tree-node-effective-code node)))
    (assert code)
    (assert (code-task code))
    (let ((*current-path* (task-tree-node-path node))
          (*current-task-tree-node* node))
      (declare (special *current-path* *current-task-tree-node*))
      (execute (code-task code))
      (join-task (code-task code)))))

(defun task-tree-node-effective-code (node)
  "Returns the effective code of the node. I.e. the code that is
  actually executed. When the node has replacements, the current
  replacement is used, otherwise the original code."
  (let ((replacements (task-tree-node-code-replacements node)))
    (if replacements
        (car replacements)
        (task-tree-node-code node))))

(defun get-ptr-parameter ()
  "Return the currently effective ptr-parameter for the current node.
   The currently effective ptr-parameter is in the car of code-replacements
   or, if there are no code-replacements, in the code of the node."
  (code-ptr-parameter (task-tree-node-effective-code (task-tree-node *current-path*))))

(defun path-next-iteration (path-part)
  (let ((iterations-spec (member :call path-part)))
    (if iterations-spec
        `(,@(subseq path-part 0 (position :call path-part)) :call ,(1+ (cadr iterations-spec)))
        (append path-part '(:call 2)))))

(defun make-task (&key (name (gensym "[MAKE-TASK]-"))
                       (sexp nil) 
                       (function nil)
                       (path *current-path*)
                       (ptr-parameter nil)
                       (is-ptr-task nil)
                       (parameters nil))
  "Returns a runnable task for the path"
  (let ((node (register-task-code sexp function :ptr-parameter ptr-parameter :path path)))
    (sb-thread:with-recursive-lock ((task-tree-node-lock node))
      (let* ((code (task-tree-node-effective-code node))
             (parameters-when-ptr (cons (code-ptr-parameter code) (cdr parameters)))
             (cr-parameters (if is-ptr-task
                                parameters-when-ptr
                                parameters)))
        (cond ((not (code-task code))
               (setf (code-parameters code) cr-parameters)
               (setf (code-task code)
                     (make-instance 'task
                       :name name
                       :thread-fun (lambda ()
                                     (apply (code-function code)
                                              cr-parameters))
                       :run-thread nil
                       :path path)))
              ((executed (code-task code))
               (make-task :name name
                          :sexp sexp
                          :function function
                          :ptr-parameter ptr-parameter
                          :is-ptr-task is-ptr-task
                          :path `(,(path-next-iteration (car path)) . ,(cdr path))
                          :parameters parameters))
              (t
               (code-task code)))))))

(defun sub-task (path)
  "Small helper function to get a sub-task of the current task."
  (make-task :name (gensym "[SUB-TASK]-") 
             :path (append path *current-path*)))

;; This function is part of the higher level interface. It's intended
;; to be used in the PARTIAL-ORDER form. That means, we do not want to
;; pass the task name as a parameter. Lower level functions should use
;; the MAKE-TASK function.
(defun task (path)
  "Small helper function to get a task from a path."
  (make-task :name (gensym "[TASK]-")
             :path path))

(defun clear-tasks (task-tree-node)
  "Removes recursively all tasks from the tree. Keeps tree structure and
   leaves code-replacements in place."
  (sb-thread:with-mutex ((task-tree-node-lock task-tree-node))
    (when (task-tree-node-code task-tree-node)
      (setf (code-task (task-tree-node-code task-tree-node)) nil
            (code-sexp (task-tree-node-code task-tree-node)) nil
            (code-function (task-tree-node-code task-tree-node)) nil
            (code-parameters (task-tree-node-code task-tree-node)) nil))
    (loop for code-replacement in (task-tree-node-code-replacements task-tree-node)
          do (setf (code-task code-replacement) nil)))
  (mapc (compose #'clear-tasks #'cdr) (task-tree-node-children task-tree-node))
  task-tree-node)

(defun task-tree-node (path &optional (node *task-tree*))
  "Returns the task-tree node for path or nil."
  (labels ((get-tree-node-internal (path node)
             (let ((child (cdr (assoc (car path) (task-tree-node-children node)
                                      :test #'equal))))
               (cond ((not (cdr path))
                      child)
                     ((not child)
                      nil)
                     (t
                      (get-tree-node-internal (cdr path) child))))))
    (get-tree-node-internal (reverse path) node)))

(defun ensure-tree-node (path &optional (task-tree *task-tree*))
  (labels ((worker (path node walked-path)
             (let ((child (cdr (assoc (car path) (task-tree-node-children node)
                                      :test #'equal)))
                   (current-path (cons (car path) walked-path)))
               (unless child
                 (sb-thread:with-recursive-lock ((task-tree-node-lock node))
                   (setf child (make-task-tree-node
                                :parent node
                                :path current-path))
                   (push (cons (car path) child)
                         (task-tree-node-children node))))
               (cond ((not (cdr path))
                      child)
                     (t
                      (worker (cdr path) child current-path))))))
    (worker (reverse path) task-tree nil)))

(defun replace-task-code (sexp function path &key (ptr-parameter nil given-ptr-parameter) (task-tree *task-tree*))
  "Adds a code replacement to a specific task tree node.

(Note: the parameters slot is refilled on each run of the plan with the parameter values actually passed
to the replaceable function. Changing the values in the parameters slot will have no effect on the plan's
running. Use the ptr-parameter slot when you want plan transformation to supply parameters to functions.)"
  (let* ((node (ensure-tree-node path task-tree))
         (old-ptr-parameter (code-ptr-parameter (task-tree-node-effective-code node)))
         (cr-ptr-parameter (if given-ptr-parameter
                               ptr-parameter
                               old-ptr-parameter)))
    (sb-thread:with-mutex ((task-tree-node-lock node))
      (push (make-code :sexp sexp :function function :ptr-parameter cr-ptr-parameter)
            (task-tree-node-code-replacements node)))))

(defun register-task-code (sexp function &key
                           (ptr-parameter nil)
                           (path *current-path*) (task-tree *task-tree*)
                           (replace-registered nil))
  "Registers a code as the default code of a specific task tree
   node. If the parameter 'replace-registered' is true, old code is
   always overwritten. Returns the node."
  (let ((node (ensure-tree-node path task-tree)))
    (sb-thread:with-recursive-lock ((task-tree-node-lock node))
      (let ((code (task-tree-node-code node)))
        (cond ((or replace-registered
                   (not code))
               (setf (task-tree-node-code node)
                     (make-code :sexp sexp :function function :ptr-parameter ptr-parameter)))
              ((or (not (code-function code))
                   (not (code-sexp code)))
               (setf (code-sexp code) sexp)
               (setf (code-function code) function)
               (setf (code-ptr-parameter code) ptr-parameter)
               (when (and (code-task code)
                          (not (executed (code-task code))))
                 (setf (slot-value (code-task code) 'thread-fun)
                       function))))))
    node))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Task tree utilities
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun is-legal-function-name-p (obj) 
  "A bit of a hack to support serialization of named functions in task tree nodes.

This is because code replacements will be inserted by named functions, whereas the 
original replaceable function macros put lambda expressions in the task tree. Also
cl-store can re/store the names of named function and have them rerunnable, but if
un-named functions are restored they will cause an error.
  
WHAT IT SHOULD DO: return true if obj is a named function (example #'+), otherwise 
false (example (lambda (&rest args) (+ args))). 

WHAT IT ACTUALLY DOES: get the string representation of obj and counts parantheses
because valid function names should not contain those."
  (eql 0
       (count #\(
              (format nil "~A" obj))))

(defun clear-code-of-illegal-function-names (code)
  (if code
    (setf (code-task code) nil))
  (if code
    (if (is-legal-function-name-p (code-function code))
        code
        (setf (code-function code) nil))
    nil))

(defun clear-illegal-function-names-internal (tree)
  (clear-code-of-illegal-function-names (task-tree-node-code tree))
  (setf (task-tree-node-code-replacements tree) 
        (mapcar #'clear-code-of-illegal-function-names (task-tree-node-code-replacements tree)))
  (setf (task-tree-node-children tree) 
        (mapcar (lambda (child-spec)
                        (cons (car child-spec) (clear-illegal-function-names-internal (cdr child-spec)))) 
                (task-tree-node-children tree)))
  tree)

(defun clear-illegal-function-names (tree)
  "Checks FUNCTION slots in CODE and CODE-TASK slots. If the function
object is a reference to an unnamed function, it is set to nil instead.

This is to allow serialization of task trees to restore code-replacements.
cl-store can't serialize something like (lambda (&rest args) &body), but
can serialize something like #'some-function, including giving the tree
the ability to call that function when needed."
;;  TODO: perhaps define a deep copy function for task trees, so that the
;;parameter of this function isn't changed by its operation.
       (clear-illegal-function-names-internal tree))

;;;
;;; STALE TASK TREE NODES
;;;
;;; Stale task tree nodes are nodes in a task tree that have no associated
;;; task object. This means they have not actually participated in the most
;;; recent execution of the high level plan the task tree belongs to. Those
;;; nodes are present in the task tree since CLEAR-TASKS leaves the task-tree
;;; structure intact and only clears all task, function, sexp and parameter
;;; slots.
;;;
;;; Stale task tree nodes are filtered for execution trace reasoning, but are
;;; needed for plan transformations.
;;;

(defun stale-task-tree-node-p (node)
  "Returns true if node is stale, i.e. it has no associated task object."
  (not (and (task-tree-node-code node)
            (code-task (task-tree-node-code node)))))

(defun filter-task-tree (predicate tree)
  "Returns a copy of the task tree which contains only nodes that satisfy
   `predicate'. CAVEAT: If a node does not satisfy `predicate' then none of
   its descendants will appear in the filtered tre, even if they satisfy
   `predicate'. Assume that the root satisfies `predicate', otherwise there
   would be no tree to return."
  (assert (funcall predicate tree))
  ;; We assume the task object has no reference to the task tree nodes (which
  ;; would be out of sync with the copied tree)
  (labels ((%do-filter (node parent)
             (make-task-tree-node :code (task-tree-node-code node)
                                  :code-replacements (task-tree-node-code-replacements node)
                                  :path (task-tree-node-path node)
                                  :parent parent
                                  :children
                                  (mapcar (lambda (child)
                                            (cons (car child)
                                                  (%do-filter (cdr child) node)))
                                          (remove-if-not predicate
                                                         (task-tree-node-children node)
                                                         :key #'cdr)))))
    (%do-filter tree nil)))

(defun flatten-task-tree (task-tree)
  "Returns a list of all the nodes in the tree."
  (cons task-tree
        (mapcan (compose #'flatten-task-tree #'cdr)
                (task-tree-node-children task-tree))))

(defun task-tree-node-parameters (task-tree-node)
  "Return the parameters with which the task was called. Assume node is not stale."
  (let ((code (task-tree-node-code task-tree-node)))
    (code-parameters code)))

(defun task-tree-node-status-fluent (task-tree-node)
  "Return the tasks status fluent. Assume node is not stale."
  (let ((code (task-tree-node-code task-tree-node)))
    (status (code-task code))))

(defun task-tree-node-result (task-tree-node)
  "Return the tasks result. Assume node is not stale."
  (let ((code (task-tree-node-code task-tree-node)))
    (result (code-task code))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Goal task tree utilities
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun goal-task-tree-node-p (task-tree-node)
  "Returns true if `task-tree-node' is a goal task."
  (let ((p (task-tree-node-path task-tree-node)))
    (and (consp p)
         (consp (car p))
         (member (caar p) '(goal goal-context)))))

(defun goal-task-tree-node-pattern (task-tree-node)
  (assert (goal-task-tree-node-p task-tree-node))
  (cadar (task-tree-node-path task-tree-node)))

(defun goal-task-tree-node-parameter-bindings (task-tree-node)
  (assert (goal-task-tree-node-p task-tree-node))
  (let ((params (task-tree-node-parameters task-tree-node))
        (pattern (goal-task-tree-node-pattern task-tree-node)))
    (mapcar (lambda (var value) (cons var value))
            (cut:vars-in pattern)
            params)))

(defun goal-task-tree-node-goal (task-tree-node)
  (assert (goal-task-tree-node-p task-tree-node))
  (let ((pattern (goal-task-tree-node-pattern task-tree-node))
        (bindings (goal-task-tree-node-parameter-bindings task-tree-node)))
    (cut:substitute-vars pattern bindings)))

