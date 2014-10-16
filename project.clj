(defproject io.aleph/dirigiste "0.1.0-alpha3"
  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"
                                     :creds :gpg}}
  :url "https://github.com/ztellman/dirigiste"
  :description "a centrally planned thread pool"
  :license {:name "MIT License"}
  :dependencies []
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.6.0"]]}}
  :java-source-paths ["src"])
