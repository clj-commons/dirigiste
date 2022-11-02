(defproject org.clj-commons/dirigiste "1.0.3"
  :deploy-repositories [["clojars" {:url "https://repo.clojars.org"
                                    :username :env/clojars_username
                                    :password :env/clojars_password
                                    :sign-releases true}]]
  :url "https://github.com/clj-commons/dirigiste"
  :description "Centrally-planned thread and object pools"
  :license {:name "MIT License"}
  :dependencies []
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.11.1"]
                                  [junit/junit "4.13"]]}
             :test {:dependencies [[junit/junit "4.13"]]}}
  :java-source-paths ["src" "test/java"]
  :test-paths ["test/clojure"]

  :javac-options ["-target" "1.8" "-source" "1.8"]
  :plugins [[lein-junit "1.1.9"]]
  :junit ["test/java"]

  ;; Maven properties for the Maven God
  :scm {:url "git@github.com:clj-commons/dirigiste.git"}
  :pom-addition ([:properties
                  [:maven.compiler.source 1.8]
                  [:maven.compiler.target 1.8]]
                 [:organization
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
