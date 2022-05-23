(defproject org.clj-commons/dirigiste "1.1.0-SNAPSHOT"
  :deploy-repositories {"releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
                                    :creds :gpg}
                        "snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"
                                     :creds :gpg}}
  :url "https://github.com/clj-commons/dirigiste"
  :description "Centrally planned thread and object pools"
  :license {:name "MIT License"}
  :dependencies []
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.11.1"]]}}
  :java-source-paths ["src"]
  :javac-options ["-target" "1.8" "-source" "1.8"]

  ;; Maven properties for the Maven God
  :scm {:url "git@github.com:clj-commons/dirigiste.git"}
  :pom-addition ([:organization
                  [:name "CLJ Commons"]
                  [:url "http://clj-commons.org/"]]
                 [:developers [:developer
                               [:id "kingmob"]
                               [:name "Matthew Davidson"]
                               [:url "http://modulolotus.net"]
                               [:email "matthew@modulolotus.net"]]])
  :classifiers {:javadoc {:java-source-paths ^:replace []
                          :source-paths ^:replace []
                          :resource-paths ^:replace []}
                :sources {:java-source-paths ^:replace ["src"]
                          :resource-paths ^:replace []}})
