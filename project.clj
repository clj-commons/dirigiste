(defproject io.aleph/dirigiste "0.1.0-alpha8"
  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"
                                     :creds :gpg}}
  :url "https://github.com/ztellman/dirigiste"
  :description "centrally planned thread and object pools"
  :license {:name "MIT License"}
  :dependencies []
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0-alpha5"]]}}
  :java-source-paths ["src"]
  :javac-options ["-target" "1.6" "-source" "1.6"])
