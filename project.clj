(defproject io.aleph/dirigiste "0.1.5"
  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"
                                     :creds :gpg}}
  :url "https://github.com/ztellman/dirigiste"
  :description "centrally planned thread and object pools"
  :license {:name "MIT License"}
  :dependencies []
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.8.0"]]}}
  :java-source-paths ["src"]
  :javac-options ["-target" "1.6" "-source" "1.6"]

  ;; Maven properties for the Maven God
  :scm {:url "git@github.com:ztellman/dirigiste.git"}
  :pom-addition [:developers [:developer
                              [:name "Zach Tellman"]
                              [:url "http://ideolalia.com"]
                              [:email "ztellman@gmail.com"]
                              [:timezone "-8"]]]
  :classifiers {:javadoc {:java-source-paths ^:replace []
                          :source-paths ^:replace []
                          :resource-paths ^:replace []}
                :sources {:java-source-paths ^:replace ["src"]
                          :resource-paths ^:replace []}})
