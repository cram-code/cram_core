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

;;; TODO: right now all these anonymous lambdas will mess up de/serialization. Will need some way to detect whether
;;; ptr-parameter slots in the task tree are safely serializable and/or provide a serialization mechanism for some
;;; reasonable class of function objects/closures.

(defparameter *deps-result* nil "Parameter to store, for a task started inside ptr-partial-order, the results
  of its deps as a list.")

(defun get-deps-result ()
  *deps-result*)

;;; Failure conditions 

(define-condition ptr-failure (plan-failure)
  ((message :initarg :message :initform nil :reader ptr-failure/message)))

(define-condition ptr-circular-partial-order (ptr-failure)
  ((cdeps :initarg :cdeps :initform nil :reader ptr-failure/cdeps)))

(define-condition ptr-malformed-partial-order (ptr-failure)
  ((deps-issue :initarg :deps-issue :initform nil :reader ptr-failure/deps-issue)))

;;; Graph classes and functions, used to detect circular dependencies in a call to ptr-partial-order
;;; These will not be exported, so we can name them however we like.

(defclass dag-vertex ()
  ((node
    :accessor dag-node
    :initarg :node
    :documentation "Identifier for the node. In the partial-order use case, will be a ptr-tag.")
   (deps
    :accessor dag-deps
    :initarg :deps
    :initform (make-hash-table :test #'eq)
    :type hash-table
    :documentation "\"Dependencies\" of a node. Key is a node identifier (eg., ptr-tag). Value is a dag-vertex [reference].")
   (users
    :accessor dag-users
    :initarg :users
    :initform (make-hash-table :test #'eq)
    :type hash-table
    :documentation "\"Users\" of a node (they have node in their deps). Key is a node identifier (eg., ptr-tag). Value is a dag-vertex [reference].")))

(defclass dag ()
  ((nodes
    :accessor dag-nodes
    :initarg :nodes
    :initform (make-hash-table :test #'eq)
    :type hash-table
    :documentation "Hash table of vertices in a graph. Key is a node identifier (eg., ptr-tag). Value is a dag-vertex [reference].")))

(defun add-vertex (dag node-id)
  "Adds an edgeless node to the dag."
  (setf (gethash node-id (dag-nodes dag)) (make-instance 'dag-vertex :node node-id)))

(defun add-dep (dag user-node-id dep-node-id)
  "In graph supplied by dag, add a dependency to user-node-id on dep-node-id.

   Will check that both node ids are present in the graph; if one misses, returns nil.

   If both present, checks whether user-node-id and dep-node-id are the same. If so,
   returns nil.

   Otherwise adds the dependency and returns true."
  (if (and (nth-value 1 (gethash user-node-id (dag-nodes dag))) (nth-value 1 (gethash dep-node-id (dag-nodes dag))) (not (eq user-node-id dep-node-id)))
      (let* ((user-node (gethash user-node-id (dag-nodes dag)))
             (dep-node (gethash dep-node-id (dag-nodes dag))))
        (setf (gethash dep-node-id (dag-deps user-node)) dep-node)
        (setf (gethash user-node-id (dag-users dep-node)) user-node)
        T)
      nil))

(defun get-free-nodes (dag)
  "Returns a list of nodes from dag which have no deps."
  (loop for node being the hash-values of (dag-nodes dag)
          when (equal (hash-table-count (dag-deps node)) 0)
            collect (dag-node node) into S
          finally (return S)))

(defun get-all-nodes-hash (dag)
  (let* ((G (make-hash-table :test #'eq)))
    (loop for node-id being the hash-keys of (dag-nodes dag) do
      (setf (gethash node-id G) node-id))
    G))

(defun get-all-nodes-list (node-hash)
  (loop for node-id being the hash-keys of node-hash
    collect node-id into G
    finally (return G)))

(defun restore-deps (dag R)
  "Restore dependencies in R into dag. R is a list of pairs (user-id dep-id). Useful because get-dag-kernel messes up the
   dep structure in the process of detecting cycles, and will call this to restore the dag."
  (loop for edge in R do
    (let* ((user-node-id (first edge))
           (dep-node-id (second edge))
           (user-node (gethash user-node-id (dag-nodes dag)))
           (dep-node (gethash dep-node-id (dag-nodes dag))))
      (setf (gethash dep-node-id (dag-deps user-node)) dep-node))))

(defun get-dag-kernel (dag &key (tail-id nil tail-id-p))
  "Returns two values:
     - a list of node-ids from dag, of nodes that cannot be topologically sorted because they have cyclic deps.
     - the list of nodes that could be topologically sorted, in some topologically sorted order.

   Use the first value to test for circular dependencies (nil if none exist).
   Use the second when you want an actual topological sort.
   
   TAIL-ID is a node-id that, along with its (indirect) dependencies, you wish to appear at the end of the
   topological order (and node-ids not (indirectly) dependent on tail-id must appear before it)."
  (let* ((S (get-free-nodes dag))
         (G (get-all-nodes-hash dag))
         (R nil)
         (L nil))
    (loop while S do
      (let* ((x (if (and tail-id-p (> (length S) 1) (eq tail-id (car S)))
                  (progn
                    (setf S (reverse S))
                    (car S))
                  (car S))))
        (setf S (cdr S))
        (setf L (cons x L))
        (remhash x G)
        (loop for user being the hash-values of (dag-users (gethash x (dag-nodes dag))) do
          (remhash x (dag-deps user))
          (setf R (cons (list (dag-node user) x) R))
          (if (equal (hash-table-count (dag-deps user)) 0)
            (setf S (cons (dag-node user) S))))))
    (restore-deps dag R)
    (values
      (get-all-nodes-list G)
      (reverse L))))

;;; Auxiliary structures to pass ptr-parameters

(defclass ptr-tag ()
  ((name
    :accessor ptr-tag/name
    :initarg :name
    :initform "PTR-TASK"
    :type (or string null)
    :documentation "A name to give the task, for logging purposes.")
   (fluent-object
    :reader ptr-tag/fluent-object
    :writer (setf ptr-tag/fluent-object-w)
;; Actually, there should be no clean way to set these slots from outside this package.
;; Reason is, the data inside is only useful for this package, and anything that
;; someone else might put here won't be valid anyway.
;;    :initarg :fluent-object
    :initform (make-fluent :name :tag-fluent :value nil)
    :type (or fluent null)
    :documentation "A fluent object used by ptr-partial-order.")
   (task-object
    :reader ptr-tag/task-object
    :writer (setf ptr-tag/task-object-w)
;; Actually, there should be no clean way to set these slots from outside this package.
;; Reason is, the data inside is only useful for this package, and anything that
;; someone else might put here won't be valid anyway.
;;    :initarg :task-object
    :initform nil
    :documentation "A task object.")))

(defgeneric wait-for-ptr-tag (tag)
  (:documentation "Wait for the fluent of a ptr-tag specified by arg to become non-nil"))

(defmethod wait-for-ptr-tag ((tag ptr-tag))
  (wait-for (ptr-tag/fluent-object tag)))

(defclass function-application ()
  ((task-tag
     :accessor function-application/task-tag
     :initarg :task-tag
     :initform nil
     :type (or ptr-tag null)
     :documentation "Reference to a ptr-tag object used to store a reference to the task this function-application will create (useful for with-task-suspended).")
   (function-object
    :accessor function-application/function-object
    :initarg :function-object
    :initform (lambda () nil)
    :type (or function compiled-function)
    :documentation "A function object to run with the supplied parameters.")
   (par-list
    :accessor function-application/par-list
    :initarg :par-list
    :initform nil
    :type list
    :documentation "A list of parameters to apply the function to.")))

(defclass function-application-list ()
  ((fn-list
    :accessor function-application-list/fn-list
    :initarg :fn-list
    :initform nil
    :type list
    :documentation "A list of function-application objects.")))

(defmacro make-fn-app (function-object &rest args)
  `(make-instance 'function-application :function-object ,function-object :par-list ,args))

(defmacro make-fn-app-list (&body body)
  "Takes a list of s-expressions and creates a function-application-list object (which can then be
   passed as a parameter for ptr-seq, ptr-par etc).

   The cars of the s-expressions in body must be named, known functions."
  `(make-instance 'function-application-list 
     :fn-list (mapcar (lambda (s-exp)
                        (make-instance 'function-application 
                                       :function-object (symbol-function (car s-exp)) 
                                       :par-list (cdr s-exp))) 
                      ',body)))

(defclass with-task-ptr-parameter ()
  ((function-application
    :accessor with-task-ptr-parameter/function-application
    :initarg :function-application
    :initform (make-instance 'function-application
                             :function-object #'identity
                             :par-list (list nil))
    :type function-application
    :documentation "A function object and parameters to apply it to, while running inside the task.")
   (class
    :accessor with-task-ptr-parameter/class
    :initarg :class
    :initform 'task
    :type (or symbol null)
    :documentation "Class of the task to start, default to 'task.")
   (name
    :accessor with-task-ptr-parameter/name
    :initarg :name
    :initform "WITH-TASK"
    :type (or string null)
    :documentation "Name of the task to start, default to \"WITH-TASK\".")))

(defclass try-each-ptr-parameter ()
  ((task-tag
     :accessor try-each-ptr-parameter/task-tag
     :initarg :task-tag
     :initform nil
     :type (or ptr-tag null)
     :documentation "Reference to a ptr-tag object used to store a reference to the task this function-application will create (useful for with-task-suspended).")
   (function-object
    :accessor try-each-ptr-parameter/function-object
    :initarg :function-object
    :initform (lambda (&rest args) (declare (ignore args)) nil)
    :type (or function compiled-function)
    :documentation "A function object to run once for each option in options-list.")
   (options-list
    :accessor try-each-ptr-parameter/options-list
    :initarg :options-list
    :initform nil
    :type (or list null)
    :documentation "Options to feed, one by one, to function-object.")))

;;(defclass par-loop-ptr-parameter ()
;;  ((function-object
;;    :reader par-loop-ptr-parameter/function-object
;;    :initarg :function-object
;;    :initform (lambda (&rest args) (declare (ignore args)) nil)
;;    :type (or function compiled-function)
;;    :documentation "A function object to run on the supplied parameter options.")
;;   (options-list
;;    :reader par-loop-ptr-parameter/options-list
;;    :initarg :options-list
;;    :initform nil
;;    :type (or list null)
;;    :documentation "Options to feed, one by one, to function-object.")))

(defmacro make-try-each-ptr-par (function-object &body body)
  "Takes a function name and lists of parameters and creates a try-each-ptr-parameter."
  `(make-instance 'try-each-ptr-parameter
                  :function-object ,function-object
                  :options-list (mapcar (lambda (arg) (list arg)) ',body)))

(defclass partial-order-ptr-parameter ()
  ((fn-apps
    :accessor partial-order-ptr-parameter/fn-apps
    :initarg :fn-apps
    :initform nil
    :type (or function-application-list null)
    :documentation "List of functions and parameters to call them with. Each element must
    have a ptr-tag.")
   (orderings
    :accessor partial-order-ptr-parameter/orderings
    :initarg :orderings
    :initform nil
    :type list
    :documentation "List of orderings. Each element is of form (user-tag dep-tag1 dep-tag2 ...).")))

;;; Conversion from fn-app-list to the dag auxiliary type

(defun get-dag-vertices (fn-app-list)
  (let* ((dag (make-instance 'dag)))
    (loop for fn-app in fn-app-list do
      (if (function-application/task-tag fn-app)
        (progn
          (if (nth-value 1 (gethash (function-application/task-tag fn-app) (dag-nodes dag)))
            (fail 'ptr-malformed-partial-order :message "PTR-PARTIAL-ORDER received two function applications with the same tag." :deps-issue fn-app))
          (add-vertex dag (function-application/task-tag fn-app)))))
    dag))

(defun add-deps (dag deps)
  (let* ((retq (mapcar (lambda (dep)
            (add-dep dag (car deps) dep))
          (cdr deps))))
    (not (position nil retq))))

(defun get-dag-fl-list (node-hash)
  (loop for node-id being the hash-keys of node-hash
    collect (ptr-tag/fluent-object node-id) into G
    finally (return G)))

(defun get-dependent-partial-order-tasks (partial-order-ptr-parameter task-ptr-tag)
  "Returns which task tags occuring in PARTIAL-ORDER-PTR-PARAMETER depend
   on (tasks that depend on) the given TASK-PTR-TAG.

   Test used is EQ.

   Returns (cons error-condition tag-list).

   TAG-LIST is a list of ptr-tag objects appeating in 
   PARTIAL-ORDER-PTR-PARAMETER with the given dependency.
   If ERROR-CONDITION, TAG-LIST is nil.
   
   ERROR-CONDITION is:
     - PTR-MALFORMED-PARTIAL-ORDER if ordering constraints refer to task tags not present in the task list,
       or contain tasks that depend on themselves.
     - PTR-CIRCULAR-PARTIAL-ORDER if ordering constraints contain circular dependencies.
     - nil otherwise."
  (let* ((fn-apps (partial-order-ptr-parameter/fn-apps partial-order-ptr-parameter))
         (fn-list (if fn-apps 
                      (function-application-list/fn-list fn-apps)
                      nil))
         (dag (get-dag-vertices fn-list))
         (malformed-orderings (loop for ordering in (partial-order-ptr-parameter/orderings partial-order-ptr-parameter)
                                when (not (add-deps dag ordering))
                                  collect ordering into R
                                finally
                                  (return R)))
         (mal-sig (if malformed-orderings
                    (make-condition 'ptr-malformed-partial-order :message "PTR-PARTIAL-ORDER received malformed ordering constraints." :deps-issue malformed-orderings)
                    nil))
         (dag-topsort (if mal-sig
                          nil
                          (multiple-value-list (get-dag-kernel dag :tail-id task-ptr-tag))))
         (dag-kernel (first dag-topsort))
         (dag-order (second dag-topsort))
         (dag-sig (if dag-kernel
                    (make-condition 'ptr-circular-partial-order :message "PTR-PARTIAL-ORDER received circular ordering constraints." :cdeps dag-kernel)
                    nil))
         (error-condition (or mal-sig dag-sig))
         (dag-relevant-order (if error-condition
                                 nil
                                 (member task-ptr-tag dag-order))))
    (cons error-condition dag-relevant-order)))

(defun delete-partial-order-task (partial-order-ptr-parameter task-ptr-tag)
  "Creates a new ptr-partial-order-parameter which is a copy of PARTIAL-ORDER-PTR-PARAMETER,
   then removes from the copy the task associated to task-ptr-tag and also removes mentions 
   to this task from all ordering relations in the copy.

   Returns the copy. PARTIAL-ORDER-PTR-PARAMETER is not affected.

   Test used is EQ.

   DOES NOT remove tasks that depend on TASK-PTR-TAG. Before deleting a task
   from a partial order, use get-dependent-partial-order-tasks to get a list
   of tasks that depend on it. That way you get to choose which to remove,
   if any."
  (let* ((fn-apps (partial-order-ptr-parameter/fn-apps partial-order-ptr-parameter))
         (fn-list (if fn-apps
                    (function-application-list/fn-list fn-apps)
                    nil))
         (orderings (partial-order-ptr-parameter/orderings partial-order-ptr-parameter)))
    (setf fn-list (remove-if (lambda (arg)
                               (eq (function-application/task-tag arg) task-ptr-tag)) 
                             fn-list))
    (setf orderings (remove-if (lambda (arg)
                                 (eq (car arg) task-ptr-tag))
                               orderings))
    (setf orderings
          (loop for ordering in orderings
            collect (remove-if (lambda (arg)
                                 (eq arg task-ptr-tag))
                               ordering)
              into orderings
            finally (return orderings)))
    (make-instance 'partial-order-ptr-parameter
                   :fn-apps (make-instance 'function-application-list
                                           :fn-list fn-list)
                   :orderings orderings)))

;;; Sequential running of function objects

(def-ptr-cram-function ptr-seq (ptr-parameter)
  "PTR-PARAMETER must be a function-application-list object.

   Run function objects from ptr-parameter in sequence. Returns the return value of the last function object.

   Function objects should be functions defined with def-[ptr-]cram-function if they are to show up in the task tree.

   Will fail as soon as one of the function objects produces a failure."
  (car (last (mapcar (lambda (fn-app)
                       (if (function-application/task-tag fn-app)
                         (let* ((s-task (make-instance 'task
                                                       :name (ptr-tag/name (function-application/task-tag fn-app))
                                                       :thread-fun (lambda () 
                                                                     (apply (function-application/function-object fn-app) 
                                                                            (function-application/par-list fn-app))))))
                           (setf (ptr-tag/task-object-w (function-application/task-tag fn-app)) s-task)
                           (join-task s-task))
                         (apply (function-application/function-object fn-app) 
                                (function-application/par-list fn-app)))) 
                     (function-application-list/fn-list ptr-parameter)))))

(def-ptr-cram-function ptr-try-in-order (ptr-parameter)
  "PTR-PARAMETER must be a function-application-list object.

   Execute function objects in ptr-parameter sequentially. Succeed if one succeeds, fail if all fail.

   Function objects should be defined with def-[ptr-]cram-function if they are to show up in the task tree.

   Return value is the return value of the first function object in ptr-parameter that succeeds.
   In case of failure on all function objects, a composite-failure is signaled."
  (block ablock
    (let* ((failures (list)))
      (mapcar (lambda (fn-app)
                (block tryout-block
                  (with-failure-handling 
                    ((plan-failure (err)
                      (setf failures (cons err failures))
                      (return-from tryout-block)))
                    (return-from ablock (if (function-application/task-tag fn-app)
                                          (let* ((s-task (make-instance 'task
                                                                        :name (ptr-tag/name (function-application/task-tag fn-app))
                                                                        :thread-fun (lambda () 
                                                                                      (apply (function-application/function-object fn-app) 
                                                                                             (function-application/par-list fn-app))))))
                                            (setf (ptr-tag/task-object-w (function-application/task-tag fn-app)) s-task)
                                            (join-task s-task))
                                          (apply (function-application/function-object fn-app) (function-application/par-list fn-app)))))))
              (function-application-list/fn-list ptr-parameter))
      (assert-no-returning
        (signal
          (make-condition 'composite-failure
                          :failures (reverse failures)))))))

(def-ptr-cram-function ptr-with-task (ptr-parameter)
  "PTR-PARAMETER is a with-task-ptr-parameter object. Slots function-application, class, name.
   
   class and name slots are optional. If not provided, they default to
   'task and \"WITH-TASK\" respectively.

   Executes function-object in a separate task and joins it."
  (let* ((function-object (function-application/function-object (with-task-ptr-parameter/function-application ptr-parameter)))
         (par-list (function-application/par-list (with-task-ptr-parameter/function-application ptr-parameter)))
         (task-class (with-task-ptr-parameter/class ptr-parameter))
         (par-name (with-task-ptr-parameter/name ptr-parameter))
         (task-name (gensym (format nil "[~a]-" par-name)))
         (task (make-instance task-class
                 :name task-name
                 :thread-fun (lambda () (apply function-object par-list)))))
    (join-task task)))

(def-ptr-cram-function ptr-with-task-suspended (ptr-parameter task &key reason)
  "PTR-PARAMETER must be a function-application-list object.
   TASK must be a ptr-tag or task object.

   Execute function objects in ptr-parameter, in sequence, with 'task' being suspended.

   Returns the value returned by the last function object in ptr-parameter.
   (NOTE: the return value is a difference to the with-task-suspended macro.)"
  (let* ((task-sym (if (typep task 'ptr-tag)
                       (ptr-tag/task-object task)
                       task))
         (retq nil))
    (unwind-protect
      (progn
        (suspend task-sym :sync t :reason reason)
        (wait-for (fl-eq (status task-sym) :suspended))
        (setf retq (car (last (mapcar (lambda (fn-app)
                                        (if (function-application/task-tag fn-app)
                                          (let* ((s-task (make-instance 'task
                                                                        :name (ptr-tag/name (function-application/task-tag fn-app))
                                                                        :thread-fun (lambda () 
                                                                                      (apply (function-application/function-object fn-app) 
                                                                                             (function-application/par-list fn-app))))))
                                            (setf (ptr-tag/task-object-w (function-application/task-tag fn-app)) s-task)
                                            (join-task s-task))
                                        (apply (function-application/function-object fn-app) 
                                               (function-application/par-list fn-app)))) 
                                      (function-application-list/fn-list ptr-parameter)))))
    (wake-up task-sym)
    retq))))

(def-ptr-cram-function ptr-try-each-in-order (ptr-parameter)
  "PTR-PARAMETER is a try-each-ptr-parameter object. Slots are function-object
   and options-list.

   options-list must contain at least one element, which must be a list of 
   parameters to pass to the function object. The function object should have
   a lambda list compatible with the parameter lists supplied by options-list.

   Applies function-object to each element in options-list sequentially until 
   function-object succeeds. Returns the result of function-object as 
   soon as it succeeds and stops iterating. Otherwise, if all attempts 
   fail, signal a composite failure.

   NOTES: 

   (1) Take care when using a ptr-cram-function as a function-object here,
   the result may not be quite what you expect. Remember that such functions 
   take their parameter from their own corresponding node, so once that node 
   exists they'll ignore cars of parameter lists passed from this function.

   (2) ptr-try-each-in-order gives you a way to convert an older cram-function
   into a ptr-cram-function without having to redefine it as one:

   (ptr-try-each-in-order (make-instance 'cpl-impl:try-each-ptr-parameter
                            :function-object some-cram-function
                            :parameter-list (list some-ptr-parameter)))

   (3) there's a difference here to the try-each-in-order macro: rather than 
   bindings to some global variables, we use parameter passing here."
  (block ablock 
    (let* ((failures (list))
           (opt-list (try-each-ptr-parameter/options-list ptr-parameter))
           (function-object (try-each-ptr-parameter/function-object ptr-parameter))
           (task-tag (try-each-ptr-parameter/task-tag ptr-parameter)))
      (dolist (arg opt-list (assert-no-returning
                              (signal
                                (make-condition 'composite-failure
                                                :failures (reverse failures)))))
        (block try-block
          (with-failure-handling 
            ((plan-failure (condition)
               (setf failures (cons condition failures))
               (return-from try-block)))
            (return-from ablock (if task-tag
                                  (let* ((s-task (make-instance 'task
                                                                :name (ptr-tag/name task-tag)
                                                                :thread-fun (lambda () 
                                                                              (apply function-object arg)))))
                                    (setf (ptr-tag/task-object-w task-tag) s-task)
                                    (join-task s-task))
                                  (apply function-object arg)))))))))

;;; Parallel running of function objects

;; We define this as a simple lisp function because we don't plan to export it
;; (it's only useful as an auxiliary inside this package) and we don't want it
;; to show up and clutter the task tree.
(defun ptr-with-parallel-children (name children-function-objects watcher-function-object)
  "Execute each of children-function-objects in parallel tasks. Execute
   watcher-function-object whenever a child changes state.

   name is used as a basis for the names of the child tasks.

   children-function-objects must be of function-application-list type

   watcher-function-object must have a lambda list containing three arguments:

   (lambda (running done failed) ...)

   The arguments are to be lists of tasks that are, respectively, running,
   completed successfully, and failed.

   All spawned child tasks are terminated when this function terminates."
  (let* ((parent-task-name-base (or name "WITH-PARALLEL-CHILDREN"))
         (parent-task-name (format nil "~a" (gensym parent-task-name-base)))
         (child-task-name-base (format nil "~a" (or name "PARALLEL")))
         (done nil)
         (done-tail nil)
         (parent-task (make-instance 'task
                        :name parent-task-name
                        :thread-fun (lambda ()
                                      (let* ((child-num (length (function-application-list/fn-list children-function-objects)))
                                             (child-numbers (alexandria:iota child-num :start 1))
                                             (retq nil)
                                             (task-list (mapcar (lambda (f-obj nr)
                                                                  (let* ((task-tag (function-application/task-tag f-obj)))
                                                                    (if task-tag
                                                                      (progn
                                                                        (setf (ptr-tag/task-object-w task-tag) 
                                                                              (make-instance 'task
                                                                                             :name (format nil "~a"
                                                                                                           (format-gensym "[~A-CHILD-#~D/~D-~a]-" child-task-name-base nr child-num (ptr-tag/name task-tag)))
                                                                                             :thread-fun (lambda () 
                                                                                                           (apply (function-application/function-object f-obj) 
                                                                                                              (function-application/par-list f-obj)))))
                                                                        (ptr-tag/task-object task-tag))
                                                                      (make-instance 'task
                                                                                     :name (format nil "~a"
                                                                                                   (format-gensym "[~A-CHILD-#~D/~D]-" child-task-name-base nr child-num))
                                                                                     :thread-fun (lambda () 
                                                                                                   (apply (function-application/function-object f-obj) 
                                                                                                          (function-application/par-list f-obj)))))))
                                                          (function-application-list/fn-list children-function-objects)
                                                          child-numbers))
                                             (cr-child-tasks (copy-list (child-tasks *current-task*)))
                                             (cr-child-status (mapcar #'status cr-child-tasks)))
                                        (declare (ignorable task-list))
                                        (wait-for (fl-apply #'notany (curry #'EQ :CREATED) cr-child-status))
                                        (whenever ((apply #'fl-or (mapcar (RCURRY #'fl-pulsed :handle-missed-pulses :once) cr-child-status)))
                                          (multiple-value-bind (running failed) 
                                                               (loop for task in cr-child-tasks
                                                                     for i from 0 below (length cr-child-tasks)
                                                                 when (and task (task-running-p task))
                                                                   collect task into running
                                                                 when (and task (task-done-p task)) do
                                                                   ;;collect task into done: we're adding to a variable defined above, and skipping nils,
                                                                   ;;because we'd like the order of tasks in done to reflect the order in which the tasks
                                                                   ;;finished. If we used a loop-local variable `done' for this purpose, the tasks would
                                                                   ;;appear in the order in which they were created, because that's how the loop iterates
                                                                   ;;on them
                                                                   (progn
                                                                     ;; use tail-tracking to add elements to the end of done. We'd like to avoid having to use reverse later
                                                                     ;; (and we cannot use nreverse when calling the watcher-function-object, since 
                                                                     ;;   we call the watcher-function-object whenever a task changes status, and
                                                                     ;;   the done list may still be in construction at that time)
                                                                     (if (not done)
                                                                       (progn
                                                                         (setf done (cons task nil))
                                                                         (setf done-tail done))
                                                                       (progn
                                                                         (setf (cdr done-tail) (cons task nil))
                                                                         (setf done-tail (cdr done-tail))))
                                                                     (setf (nth i cr-child-tasks) nil))
                                                                 when (and task (task-failed-p task))
                                                                   collect task into failed
                                                                 ;;finally (return (values running done failed)))
                                                                 finally (return (values running failed)))
                                            (if (member 
                                                  (make-keyword (string-upcase parent-task-name-base)) 
                                                  +available-log-tags+)
                                              (%log-event "~a" (list parent-task-name-base) 
                                                          "~@[R: ~{~A~^, ~} ~] ~@[~:_D: ~{~A~^, ~} ~] ~@[~:_F: ~{~A~^, ~}~]"
                                                          (list (mapcar #'task-abbreviated-name running)
                                                                (mapcar #'task-abbreviated-name done)
                                                                (mapcar #'task-abbreviated-name failed)))
                                              nil)
                                            (setf retq (funcall watcher-function-object running done failed))
                                            (if (not running)
                                              (return retq)))))))))
    (join-task parent-task)))

(def-ptr-cram-function ptr-par (ptr-parameter)
  "PTR-PARAMETER is a function-application-list object.
   Executes function objects in parallel. Fails if one fails. Succeeds if all
   succeed. Returns the result of the task that finished last."
  (block ptr-par-block
    (ptr-with-parallel-children "PAR" 
                                ptr-parameter
                                (lambda (running done failed)
                                  (cond (failed
                                          (assert-no-returning
                                            (signal (result (car failed)))))
                                        ((not running)
                                          (result (car (last done)))))))))

;; a bit of a hack to force a return from ptr-with-parallel-children when one task completes
;; in a PURSUE or TRY-ALL block.
(define-condition pursue-done (plan-failure)
  ((result :initarg :result :initform nil :reader pursue-done/result))) 

(defun evaporate-subts (task)
  (mapcar #'evaporate-subts (child-tasks task))
  (format T "Task: ~a~%" task)
  (evaporate task))

(def-ptr-cram-function ptr-pursue (ptr-parameter)
  "PTR-PARAMETER is a function-application-list object.
   Executes function objects in parallel. Fails if one fails. Succeeds if one
   succeeds, and returns the value returned by the first successful task."
  (block ptr-pursue-block
    ;; a bit of a hack to force a return from ptr-with-parallel-children when one task completes.
    (with-failure-handling 
      ((pursue-done (e)
        (mapcar #'evaporate (child-tasks *current-task*))
        (return-from ptr-pursue-block (pursue-done/result e))))
      (ptr-with-parallel-children "PURSUE" 
                                  ptr-parameter
                                  (lambda (running done failed)
                                    (declare (ignore running))
                                    (cond (failed
                                            (assert-no-returning
                                              (signal (result (car failed)))))
                                          (done
                                            (assert (eq (value (status (car done))) :succeeded))
                                            (result (car done))
                                            (assert-no-returning (signal (make-condition 'pursue-done :result (result (car done))))))))))))

(def-ptr-cram-function ptr-try-all (ptr-parameter)
  "PTR-PARAMETER is a function-application-list object.
   Executes function objects in parallel. Fails if all fail. Succeeds if one
   succeeds, and returns the value returned by the first successful task.

   In case of failure, a condition of type 'composite-failure' is signaled, 
   containing the list of all error messages and data."
  (block ptr-try-all-block
    ;; a bit of a hack to force a return from ptr-with-parallel-children when one task completes.
    (with-failure-handling 
      ((pursue-done (e)
        (mapcar #'evaporate (child-tasks *current-task*))
        (return-from ptr-try-all-block (pursue-done/result e))))
      (ptr-with-parallel-children "TRY-ALL" 
                                  ptr-parameter
                                  (lambda (running done failed)
                                    (cond ((and (not running) (not done) failed)
                                            (assert-no-returning
                                              (signal
                                                (make-condition 'composite-failure :failures (mapcar #'result failed)))))
                                          (done
                                            (assert-no-returning
                                              (signal
                                                (make-condition 'pursue-done :result (result (car done))))))))))))

(defun ptr-par-loop-internal (ptr-parameter)
  (block ptr-par-loop-block
    (let* ((ptr-parameter-adjusted (make-instance 'function-application-list
                                                  :fn-list (mapcar (lambda (arg)
                                                                     (make-instance 'function-application
                                                                                    :function-object (try-each-ptr-parameter/function-object ptr-parameter)
                                                                                    :par-list arg))
                                                                   (try-each-ptr-parameter/options-list ptr-parameter)))))
      (ptr-with-parallel-children "PAR-LOOP" 
                                  ptr-parameter-adjusted
                                  (lambda (running done failed)
                                    (cond (failed
                                            (assert-no-returning
                                              (signal (result (car failed)))))
                                          ((not running)
                                            (result (car (last done))))))))))

(def-ptr-cram-function ptr-par-loop (ptr-parameter)
  "PTR-PARAMETER is a try-each-ptr-parameter object. Slots are function object
  and options-list.

  For each element in options-list, runs (apply function-object element) in parallel.
  Fails if one fails. Succeeds if all succeed. Returns the result of the task that finished last."
  (if (try-each-ptr-parameter/task-tag ptr-parameter)
    (let* ((task-tag (try-each-ptr-parameter/task-tag ptr-parameter))
           (s-task (make-instance 'task
                                  :name (ptr-tag/name task-tag)
                                  :thread-fun (lambda () 
                                                (ptr-par-loop-internal ptr-parameter)))))
      (setf (ptr-tag/task-object-w task-tag) s-task)
      (join-task s-task))
    (ptr-par-loop-internal ptr-parameter)))
  
(def-ptr-cram-function ptr-partial-order (ptr-parameter)
  "PTR-PARAMETER must be a partial-order-ptr-parameter object,
   with slots fn-apps and orderings.

   fn-apps contains a list of function applications. Each function
   application contains a function object and a list of parameters
   to apply it to. Each function application should also contain a 
   reference to a ptr-tag object if it is to be part of or be
   affected by ordering constraints; a tagless function application
   will just run in parallel to the others.

   orderings contains a list, where each element is of the form
   
   (user-tag dep-tag1 dep-tag2 ...)

   and the interpretation is the following: before the task associated
   to user-tag can run, tasks associated to dep-tag1, dep-tag2 etc.
   must successfully complete.

   A task tag is an object whose contents will be manipulated by the
   ptr functions. Do not rely on data placed by you the user in there
   to remain.

   Runs the fn-apps in parallel, respecting the ordering conditions,
   IF the ordering conditions are well specified and non-circular.

   To be well-specified, ordering conditions must:

   - refer only to ptr-tags referenced in fn-apps.
   - no tag can depend on itself

   Will emit a failure if:
   
   - orderings not well specified or circular
   - one of the function applications fails

   Otherwise returns the value returned by the last task to finish."
  (let* ((fn-apps (partial-order-ptr-parameter/fn-apps ptr-parameter))
         (fn-list (if fn-apps
                      (function-application-list/fn-list fn-apps)
                      nil))
         (orderings (partial-order-ptr-parameter/orderings ptr-parameter))
         (dag (get-dag-vertices fn-list))
         (malformed-orderings (loop for ordering in orderings
                                when (not (add-deps dag ordering))
                                  collect ordering into R
                                finally
                                  (return R)))
         (mal-sig (if malformed-orderings
                    (fail 'ptr-malformed-partial-order :message "PTR-PARTIAL-ORDER received malformed ordering constraints." :deps-issue malformed-orderings)
                    nil))
         (dag-kernel (get-dag-kernel dag))
         (dag-sig (if dag-kernel
                    (fail 'ptr-circular-partial-order :message "PTR-PARTIAL-ORDER received circular ordering constraints." :cdeps dag-kernel)
                    nil))
         (ptr-tags (loop for fn-app in fn-list
                     when (function-application/task-tag fn-app)
                       collect (function-application/task-tag fn-app) into R
                     finally
                       (return R)))
         (ptr-parameter-adjusted (make-instance 'function-application-list)))
    (declare (ignore mal-sig) (ignore dag-sig))
;; Once all tests on well-formedness and circularity are done, we can proceed with the actual construction of tasks.

;; First, initialize the ptr-tag objects to contain new fluents, each of which is initialized to nil. We do this now, because it means
;; these fluents are guaranteed to exist, and therefore make meaningful targets to wait on, when we generate tasks later.
    (loop for ptr-tag in ptr-tags do
      (setf (ptr-tag/fluent-object-w ptr-tag) (make-fluent :name :ptr-tag-fluent :value nil)))
;; Second, when we generate tasks, we will, for each task:
;; - prepend waits on fluents from deps ptr-tags. We wait on the fluents (which we know exist at this stage) rather than task fluents,
;;   which are not yet constructed
;; - append a setting of the fluent from the corresponding ptr-tag to T.
    (loop for fn-app in fn-list do
      (let* ((task-tag (function-application/task-tag fn-app))
             (fn-ob (function-application/function-object fn-app))
             (par-list (function-application/par-list fn-app))
             (dag-node (if task-tag
                         (gethash task-tag (dag-nodes dag))
                         nil))
             (dep-fl-list (if dag-node
                            (get-dag-fl-list (dag-deps dag-node))
                            nil))
             (dep-tag-list (if dag-node
                             (mapcar #'dag-node (get-all-nodes-list (dag-deps dag-node)))
                             nil))
             (have-deps (if dep-fl-list T nil)))
        (setf (function-application-list/fn-list ptr-parameter-adjusted)
              (cons (make-instance 'function-application 
                                   :task-tag task-tag
                                   :function-object (lambda (&rest args)
                                                      (let* ((retq nil)
                                                             (dep-fluent (if dep-fl-list
                                                                           (apply #'fl-funcall
                                                                                  (cons
                                                                                    (lambda (&rest args)
                                                                                      (equal (position nil args) nil))
                                                                                    dep-fl-list))
                                                                           (make-fluent :name :ptr-tag-fluent :value T))))
                                                        (if have-deps 
                                                            (progn 
                                                              (wait-for dep-fluent)
                                                              (setf *deps-result*
                                                                    (mapcar (lambda (a-tag)
                                                                              (list
                                                                                (ptr-tag/name a-tag)
                                                                                (ptr-tag/task-object a-tag))) 
                                                                            dep-tag-list))))
                                                        (setf retq (apply fn-ob args))
                                                        (if task-tag
                                                          (setf (value (ptr-tag/fluent-object task-tag)) T))
                                                        retq))
                                   :par-list par-list)
                    (function-application-list/fn-list ptr-parameter-adjusted)))))
;; Third, run the created task functions in parallel.
    (block ptr-partial-order-block
      (ptr-with-parallel-children "PARTIAL-ORDER" 
                                  ptr-parameter-adjusted
                                  (lambda (running done failed)
                                    (cond (failed
                                            (assert-no-returning
                                              (signal (result (car failed)))))
                                          ((not running)
                                            (result (car (last done))))))))))

