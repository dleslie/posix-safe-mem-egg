(module posix-safe-mem *

        (import scheme chicken)
        (use posix lolevel posix-shm posix-semaphore uuid)

        ;; Store PSHM object
        (define-record safe-mem 
          semaphore semaphore-name memory-size shared-memory shared-memory-path memory-map memory-map-protection memory-map-flags copy pass-count #|grow shrink |#)

        (define-record-printer (safe-mem m p)
          (fprintf p "#,<safe-mem semaphore: ~S semaphore-name: ~S memory-size: ~S shared-memory: ~S shared-memory-path: ~S memory-map: ~S copy: ~S pass-count:~S>"
                   (safe-mem-semaphore m) (safe-mem-semaphore-name m) (safe-mem-memory-size m) (safe-mem-shared-memory m) (safe-mem-shared-memory-path m) (safe-mem-memory-map m) (safe-mem-copy m) (safe-mem-pass-count m)))

        (define (lock-safe-mem safe-mem)
          (when (>= 0 (safe-mem-pass-count safe-mem))
                (sem-wait (safe-mem-semaphore safe-mem)))
          (safe-mem-pass-count-set! safe-mem (+ 1 (safe-mem-pass-count safe-mem))))

        (define (unlock-safe-mem safe-mem)
          (when (< 0 (safe-mem-pass-count safe-mem))
                (safe-mem-pass-count-set! safe-mem (- 1 (safe-mem-pass-count safe-mem))))
          ;; pass-count does not reside in shared memory, and so is a process-local variable
          (when (>= 0 (safe-mem-pass-count safe-mem))
                (sem-post (safe-mem-semaphore safe-mem))))

        (define-syntax with-safe-mem
          (syntax-rules ()
            ((with-safe-mem safe-mem body ...)
             (dynamic-wind
                 (lambda () (lock-safe-mem safe-mem))
                 (lambda () body ...)
                 (lambda () (unlock-safe-mem safe-mem))))))

        ;; get: lambda returns value, using unevict if copy and pointer->object if not
        (define (safe-mem-get safe-mem)
          (let ((primary (pointer->object (memory-mapped-file-pointer (safe-mem-memory-map safe-mem)))))
            (if (safe-mem-copy safe-mem)
                (object-unevict primary #t)
                primary)))

        (define (safe-mem-set! safe-mem value)
          (let ((size (object-size value)))
            (assert (< 0 size) "Shared object size must be greater than zero. (Did you try to share an immediate object?)")
            (assert (<= size (safe-mem-memory-size safe-mem)) (format "New value size (~A) must be less than or equal to safe mem memory size (~A)." size (safe-mem-memory-size safe-mem))))
          ;; Write the new value to the location
          (receive (v p) 
                   (object-evict-to-location value 
                                             (memory-mapped-file-pointer (safe-mem-memory-map safe-mem)))
                   v))

        (define (safe-mem-get/lock safe-mem)
          (with-safe-mem safe-mem (safe-mem-get safe-mem)))

        (define (safe-mem-set!/lock safe-mem value)
          (with-safe-mem safe-mem (safe-mem-set! safe-mem value)))

        (define (safe-mem-truncate! safe-mem #!optional (size #f))
          (when (not size)
                (set! size (safe-mem-memory-size safe-mem)))
          (file-truncate (safe-mem-shared-memory safe-mem) size))

        (define (safe-mem-resize! safe-mem new-size)
          (safe-mem-memory-size-set! safe-mem new-size)
          (safe-mem-truncate! safe-mem new-size)
          (safe-mem-remap! safe-mem))

        (define (safe-mem-remap! safe-mem)
          (when (safe-mem-memory-map safe-mem)
                (unmap-file-from-memory (safe-mem-memory-map safe-mem)))
          (let ((mmap (map-file-to-memory #f (safe-mem-memory-size safe-mem) 
                                          (safe-mem-memory-map-protection safe-mem) 
                                          (safe-mem-memory-map-flags safe-mem) 
                                          (safe-mem-shared-memory safe-mem))))
            (safe-mem-memory-map-set! safe-mem mmap)))

        (define make-safe-mem
          (let ((%make make-safe-mem))
            (lambda (value 
                     #!key (semaphore-name (sprintf "/safe-mem-semaphore-~A" (uuid-v4)))
                     (shared-memory-name (sprintf "/safe-mem-shared-memory-~A" (uuid-v4)))
                     (shared-memory-flags (list open/creat open/rdwr))
                     (semaphore-flags o/creat)
                     (semaphore-mode 0644)
                     (memory-map-protection (bitwise-ior prot/read prot/write prot/exec))
                     (memory-map-flags map/shared)
                     (size 0)
                     (copy #t))
              (let ((size (if (= 0 size) (object-size value) size)))
                (assert (< 0 size) "Shared object size must be greater than zero. (Did you try to share an immediate object?)")
                (let* ((mmap #f)
                       (pass-count 0)
                       (safe-mem (%make (sem-open/mode semaphore-name semaphore-flags semaphore-mode 1)
                                        semaphore-name
                                        size
                                        (shm-open shared-memory-name shared-memory-flags)
                                        shared-memory-name
                                        mmap
                                        memory-map-protection
                                        memory-map-flags
                                        copy
                                        pass-count)))
                  (safe-mem-truncate! safe-mem)
                  (safe-mem-remap! safe-mem)
                  (safe-mem-set! safe-mem value)
                  safe-mem)))))

        (define (free-safe-mem! safe-mem)
          (and
           (when (safe-mem-memory-map safe-mem)
                 (unmap-file-from-memory (safe-mem-memory-map safe-mem)))
           (when (safe-mem-shared-memory safe-mem)
                 (shm-unlink (safe-mem-shared-memory-path safe-mem)))
           (when (safe-mem-semaphore safe-mem)
                 (and
                  (sem-close (safe-mem-semaphore safe-mem))
                  (sem-unlink (safe-mem-semaphore-name safe-mem))))
           #t))

        (define (swap-safe-mem! a b)
          (with-safe-mem 
           a 
           (with-safe-mem 
            b
            (if (safe-mem-copy a)
                (let* ((t (safe-mem-get a))
                       (t2 (safe-mem-get b)))
                  (safe-mem-set! a (if (safe-mem-copy b) t2 (object-evict t2)))
                  (safe-mem-set! b t))
                (let* ((t (object-evict (safe-mem-get a)))
                       (t2 (safe-mem-get b)))
                  (safe-mem-set! a (if (safe-mem-copy b) t2 (object-evict t2)))
                  (safe-mem-set! b t)))))
          #t)
)
