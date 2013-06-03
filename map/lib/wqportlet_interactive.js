Ext.BLANK_IMAGE_URL = 'http://secoora.org/resources/images/default/s.gif';


Ext.namespace('rcoosmapping');

GeoExt.Popup.prototype.getState = function()  { return null; }
Ext.override(Ext.data.Connection, {


	request : function(o){
        if(this.fireEvent("beforerequest", this, o) !== false){
            var p = o.params;

            if(typeof p == "function"){
                p = p.call(o.scope||window, o);
            }
            if(typeof p == "object"){
                p = Ext.urlEncode(p);
            }
            if(this.extraParams){
                var extras = Ext.urlEncode(this.extraParams);
                p = p ? (p + '&' + extras) : extras;
            }

            var url = o.url || this.url;
            if(typeof url == 'function'){
                url = url.call(o.scope||window, o);
            }

            if(o.form){
                var form = Ext.getDom(o.form);
                url = url || form.action;

                var enctype = form.getAttribute("enctype");
                if(o.isUpload || (enctype && enctype.toLowerCase() == 'multipart/form-data')){
                    return this.doFormUpload(o, p, url);
                }
                var f = Ext.lib.Ajax.serializeForm(form);
                p = p ? (p + '&' + f) : f;
            }

            var hs = o.headers;
            if(this.defaultHeaders){
                hs = Ext.apply(hs || {}, this.defaultHeaders);
                if(!o.headers){
                    o.headers = hs;
                }
            }

            var cb = {
                success: this.handleResponse,
                failure: this.handleFailure,
                scope: this,
                argument: {options: o},
                timeout : this.timeout
            };

            var method = o.method||this.method||(p ? "POST" : "GET");

            if(method == 'GET' && (this.disableCaching && o.disableCaching !== false) || o.disableCaching === true){
                url += (url.indexOf('?') != -1 ? '&' : '?') + '_dc=' + (new Date().getTime());
            }

            if(typeof o.autoAbort == 'boolean'){ // options gets top priority
                if(o.autoAbort){
                    this.abort();
                }
            }else if(this.autoAbort !== false){
                this.abort();
            }
            if((method == 'GET' && p) || o.xmlData || o.jsonData){
                url += (url.indexOf('?') != -1 ? '&' : '?') + p;
                p = '';
            }
            if (o.scriptTag) {
               this.transId = this.scriptRequest(method, url, cb, p, o);
            } else {
               this.transId = Ext.lib.Ajax.request(method, url, cb, p, o);
            }
            return this.transId;
        }else{
            Ext.callback(o.callback, o.scope, [o, null, null]);
            return null;
        }
    },

    scriptRequest : function(method, url, cb, data, options) {
        var transId = ++Ext.data.ScriptTagProxy.TRANS_ID;
        var trans = {
            id : transId,
            cb : options.callbackName || "stcCallback"+transId,
            scriptId : "stcScript"+transId,
            options : options
        };
        if(data !== undefined)
        {
          url += (url.indexOf("?") != -1 ? "&" : "?") + data + String.format("&{0}={1}", options.callbackParam || 'callback', trans.cb);
        }
        else
        {
          url += (url.indexOf("?") != -1 ? "" : "?") + String.format("&{0}={1}", options.callbackParam || 'callback', trans.cb);
        }
        var conn = this;
        window[trans.cb] = function(o){
            conn.handleScriptResponse(o, trans);
        };

//      Set up the timeout handler
        trans.timeoutId = this.handleScriptFailure.defer(cb.timeout, this, [trans]);

        var script = document.createElement("script");
        script.setAttribute("src", url);
        script.setAttribute("type", "text/javascript");
        script.setAttribute("id", trans.scriptId);
        document.getElementsByTagName("head")[0].appendChild(script);

        return trans;
    },

    handleScriptResponse : function(o, trans){
        this.transId = false;
        this.destroyScriptTrans(trans, true);
        var options = trans.options;

//      Attempt to parse a string parameter as XML.
        var doc;
        if (typeof o == 'string') {
            if (window.ActiveXObject) {
                //var doc = new ActiveXObject("Microsoft.XMLDOM");
                doc = new ActiveXObject("Microsoft.XMLDOM");
                doc.async = "false";
                doc.loadXML(o);
            } else {
                //var doc = new DOMParser().parseFromString(o,"text/xml");
                doc = new DOMParser().parseFromString(o,"text/xml");
            }
        }

//      Create the bogus XHR
        response = {
            responseObject: o,
            responseText: (typeof o == "object") ? Ext.util.JSON.encode(o) : String(o),
            responseXML: doc,
            argument: options.argument
        };
        this.fireEvent("requestcomplete", this, response, options);
        Ext.callback(options.success, options.scope, [response, options]);
        Ext.callback(options.callback, options.scope, [options, true, response]);
    },

    handleScriptFailure: function(trans) {
        this.trans = false;
        this.destroyScriptTrans(trans, false);
        var options = trans.options;
        response = {
        	argument:  options.argument
        };
        this.fireEvent("requestexception", this, response, options, new Error("Timeout"));
        Ext.callback(options.failure, options.scope, [response, options]);
        Ext.callback(options.callback, options.scope, [options, false, response]);
    },

    // private
    destroyScriptTrans : function(trans, isLoaded){
        document.getElementsByTagName("head")[0].removeChild(document.getElementById(trans.scriptId));
        clearTimeout(trans.timeoutId);
        if(isLoaded){
            window[trans.cb] = undefined;
            try{
                delete window[trans.cb];
            }catch(e){}
        }else{
            // if hasn't been loaded, wait for load to remove it to prevent script error
            window[trans.cb] = function(){
                window[trans.cb] = undefined;
                try{
                    delete window[trans.cb];
                }catch(e){}
            };
        }
    }
});

rcoosmapping.wqResultsPopup = Ext.extend(GeoExt.Popup,{
  feature : undefined,

  constructor: function(config)
  {
    this.listeners = {afterrender : this.afterrender};
    this.feature = config.feature;
    rcoosmapping.wqResultsPopup.superclass.constructor.call(this,config);
  },
  /*
  Function: afterrender
  Purpose: Event fires after this window renders. We can then correctly size our tab to fit into the window.
  */
  afterrender : function()
  {
    var tabPanel = new Ext.TabPanel({
                      resizeTabs: true,
                      width: this.getInnerWidth(),
                      height: this.getInnerHeight(),
                      id: 'tabPanel',
                      activeTab: 0
                     });
    this.add(tabPanel);
    var resultsPanel = new Ext.Panel(
      {
        id: 'wqResultsPanelPopup',
        title: "Prediction Results",
          autoScroll: true,
        html : this.feature.attributes.description
      });
    tabPanel.add(resultsPanel);
    if(this.feature.attributes.data != undefined)
    {
      var dataPanel = new Ext.Panel(
        {
          id: 'wqDataPanelPopup',
          title: "Data Used",
          autoScroll: true,
          html : this.feature.attributes.data.value
        });
      tabPanel.add(dataPanel);
    }
  }

});
rcoosmapping.wqResultsLayer  = Ext.extend(OpenLayers.Layer.Vector,{
  ctSelectFeature : undefined,
  popup : undefined,
  mapObj : undefined,
  googAnalytics : undefined,

  constructor: function(name, options)
  {
    rcoosmapping.wqResultsLayer.superclass.constructor.call(this, name, options);
  },
  setGoogleAnalytics : function(googAnalytics)
  {
    this.googAnalytics = googAnalytics;
  },

  createSelectFeature : function(mapObj)
  {
    this.mapObj = mapObj;
    ctSelectFeature = new OpenLayers.Control.SelectFeature([this],
    {
      onSelect: this.kmlClick,
      scope: this
    });
    this.mapObj.addControl(ctSelectFeature);
    ctSelectFeature.activate();
  },
  clearPopup : function()
  {
    if(this.popup != undefined)
    {
      this.popup.close();
      this.popup = undefined;
    }
  },
  kmlClick: function(feature)
  {
    this.clearPopup();

    var title;
    if(feature.attributes.station != undefined)
    {
      title = feature.attributes.station.value + " Prediction";
    }
    else
    {
      title = "Prediction";
    }
    this.popup = new rcoosmapping.wqResultsPopup({
        id: 'wqPredictionPopup',
        map: this.mapObj,
        title: title,
        autoScroll: true,
        location: feature,
        width: 375,
        height: 200,
        collapsible: true,
        anchored: true,
        feature : feature
    });
    this.popup.show();
    if(this.googAnalytics != undefined)
    {
      var stationName = Ext.util.Format.trim(feature.attributes.station.value);
      this.googAnalytics.trackEvent("WQ Station", "Click", stationName);
    }
  }
});



rcoosmapping.wqPortletMap = Ext.extend(rcoosmapping.olMap,
{
  hfradarPlatforms : null,
  insituPlatforms : null,

  constructor: function(config)
  {
    rcoosmapping.wqPortletMap.superclass.constructor.call(this, config);
  },
  addLayers : function(configParams)
  {
    rcoosmapping.wqPortletMap.superclass.addLayers.call(this, configParams);
    var wqResults = new rcoosmapping.wqResultsLayer("Water Quality Results",
      {
        strategies: [new OpenLayers.Strategy.Fixed()],
        projection: new OpenLayers.Projection("EPSG:4326"),
        protocol: new OpenLayers.Protocol.HTTP({
            url: "http://129.252.139.124/mapping/xenia/feeds/dhec/etcocPredictions.kml",
            format: new OpenLayers.Format.KML({
                extractStyles: true,
                extractAttributes: true,
                maxDepth: 3
            })
        }),
        GROUP: 'Results',
        visibility: true,
        QUERYABLE: true
      });
      wqResults.setGoogleAnalytics(this.googAnalytics);

    this.olMap.addLayer(wqResults);
    wqResults.createSelectFeature(this.olMap);

      var watershedStyles = new OpenLayers.StyleMap({
          "default": new OpenLayers.Style({
              fillColor: "#ffcc66",
              strokeColor: "#ff9933",
              strokeWidth: 2,
              graphicZIndex: 1
          }),
          "select": new OpenLayers.Style({
              fillColor: "#66ccff",
              strokeColor: "#3399ff",
              graphicZIndex: 2
          })
      });
      watersheds = new OpenLayers.Layer.GML("Watershed Boundaries",
          "http://secoora.org/wqportlet/data/kml/watershed.kml",
          {
            format: OpenLayers.Format.KML,
            projection: new OpenLayers.Projection("EPSG:4326"),
            formatOptions:
            {
              extractStyles: false,
              extractAttributes: true
            },
            GROUP: 'Overlays',
            visibility:false,
            styleMap: watershedStyles
          }
       );
      this.olMap.addLayer(watersheds);


  },

  treeNodeCheckChange : function(node, checked)
  {
    if(this.googAnalytics !== undefined && checked)
    {
      if(this.mapInited === true)
      {
        if(this.googAnalytics != undefined)
        {
          this.googAnalytics.trackEvent("Interactive Map Layers", "Click", node.layer.name);
        }
      }
    }
  },

  createToolbar : function(groupName)
  {
    var createSeparator = function(toolbarItems)
    {
       toolbarItems.push(" ");
       toolbarItems.push("-");
       toolbarItems.push(" ");
    };

    action = new GeoExt.Action({
        control: new OpenLayers.Control.ZoomToMaxExtent(),
        map: this.olMap,
        iconCls: 'zoomfull',
        group: groupName,
        enableToggle: true,
        tooltip: 'Zoom to full extent of the map.',
        tooltipType: 'title'
    });

    this.toolbarItems.push(action);

    createSeparator(this.toolbarItems);

    action = new GeoExt.Action({
        control: new OpenLayers.Control.ZoomBox(
        ),
        toggleHandler : function(actionObj, checked) {
          if(this.insituPlatforms !== null)
          {
            if(checked)
            {
              this.insituPlatforms.enableToolTips(false);
            }
            else
            {
              this.insituPlatforms.enableToolTips(true);
            }
          }

        },
        //text: 'Zoom In',
        scope: this,
        map: this.olMap,
        iconCls: 'zoomin',
        toggleGroup: groupName,
        enableToggle: true,
        tooltip: 'Zoom in: click in the olMap or use the left mouse button and drag to create a rectangle',
        tooltipType: 'title'
    });

    this.toolbarItems.push(action);

    //We use the activate/deactive events to enable/disable the tooltips on the platforms while the user tries to zoom in.
    action = new GeoExt.Action({
        control: new OpenLayers.Control.ZoomBox({
            out: true
        }),
        toggleHandler : function(actionObj, checked) {
          if(this.insituPlatforms !== null)
          {
            if(checked)
            {
              this.insituPlatforms.enableToolTips(false);
            }
            else
            {
              this.insituPlatforms.enableToolTips(true);
            }
          }
        },
        scope: this,
        map: this.olMap,
        iconCls: 'zoomout',
        toggleGroup: groupName,
        enableToggle: true,
        tooltip: 'Zoom out: click in the olMap or use the left mouse button and drag to create a rectangle',
        tooltipType: 'title'
    });

    this.toolbarItems.push(action);

    //We use the activate/deactive events to enable/disable the tooltips on the platforms while the user tries to zoom out.
    action = new GeoExt.Action({
        control: new OpenLayers.Control.DragPan({
            isDefault: false
        }),
        map: this.olMap,
        iconCls: 'pan',
        toggleGroup: groupName,
        enableToggle: true,
        tooltip: 'Pan olMap: keep the left mouse button pressed and drag the olMap',
        tooltipType: 'title'
    });

    this.toolbarItems.push(action);

    createSeparator(this.toolbarItems);

    action = new Ext.Action({
        //control: new OpenLayers.Control.WMSGetFeatureInfo({
        //    isDefault: true
        //}),
        handler: function(){
              if(this.insituPlatforms !== null)
              {
                this.focus();
              }
            },
        scope: this,
        //map: this.olMap,
        iconCls: 'info',
        toggleGroup: groupName,
        enableToggle: true,
        tooltip: 'For layers with querying ability, this issues a request for the data',
        tooltipType: 'title'
    });

    this.toolbarItems.push(action);

    createSeparator(this.toolbarItems);
    action = new Ext.Action({
        tooltip: 'General Map Help',
        handler: function()
        {
          window.open('./maphelp.html');
        },
        iconCls: 'help',
        scope: this,
        tooltip: 'Launch the help window',
        tooltipType: 'title'
    });
    this.toolbarItems.push(action);
  },
  init : function(dataServerIP,mapservIP,tilecacheIP,configParams)
  {
    rcoosmapping.wqPortletMap.superclass.init.call(this, dataServerIP,mapservIP,tilecacheIP,configParams);
  }
});


rcoosmapping.app = function() {
  this.viewport;
  this.mapTabs;
  this.dataServerIP;
  this.mapservIP;
  this.tilecacheIP;
  this.googAnalytics;
  return {
    processConfig : function(response, options)
    {

      var jsonObject = Ext.util.JSON.decode(response.responseText);

      this.googAnalytics = new googleAnalytics(jsonObject.googleAnalyticsKey);
      if(this.googAnalytics.getTracker() !== null)
      {
        //Track the page view
        this.googAnalytics.trackPageView();
      }

      var mapTabs = jsonObject.tabs;



      var len = mapTabs.length;
      var i;
      var tabs = [];
      for(i = 0; i < len; i++)
      {
        var tabOptions = mapTabs[i];
        var mapObj = new rcoosmapping.wqPortletMap();
        //mapObj.setAnalytics(this.googAnalytics);
        tabOptions.googAnalytics = this.googAnalytics;
        tabOptions.proxyHost = jsonObject.serverSettings.proxyHost;
        mapObj.init(this.dataServerIP,this.mapservIP,this.tilecacheIP, tabOptions);
        //mapObj.createPanel(tabOptions.name);
        tabs.push(mapObj);
        mapObj.mapInitialized(true);

      }
      this.mapTabs = new Ext.TabPanel({
        region: 'center',
        activeTab: 0,
        deferredRender: true,
        items: tabs
      });
      this.viewport = new Ext.Viewport({
          cls: 'map-panel',
          layout:'border',
          items:[
            new Ext.BoxComponent({ // raw
                region: 'north',
                el: 'header',
                style: 'background-color: #FFFFFF;'
              }),
            {
              region: 'center',
              layout: 'border',
              items: [this.mapTabs]
            }
          ]
      });
      var mapObj  = this.mapTabs.get(0)
      if(tabOptions.mapConfig.mapExtents.zoomToExtent !== undefined)
      {
        mapObj.olMap.zoomToExtent(
            new OpenLayers.Bounds(
                tabOptions.mapConfig.mapExtents.zoomToExtent.lowerLeft.lon,
                tabOptions.mapConfig.mapExtents.zoomToExtent.lowerLeft.lat,
                tabOptions.mapConfig.mapExtents.zoomToExtent.upperRight.lon,
                tabOptions.mapConfig.mapExtents.zoomToExtent.upperRight.lat
            ).transform(mapObj.olMap.displayProjection, mapObj.olMap.projection)
        );

      }

      this.mapTabs.on({
        tabchange: function(panel,tab)
        {
          //Run through the tabs and close any open popups.
          var i;
          for(i = 0; i < panel.items.length; i++)
          {
            var curTab = panel.items.get(i);
            curTab.clearPopup();
          }
        }
      });
    },

    init: function(dataServerIP,mapservIP,tilecacheIP,jsonLayerCfgFile) {
      //Create the googleAnalytics object. We use it to track page view as well as other events such as what layer the user choose or platform
      //the user clicks on.
      this.dataServerIP = dataServerIP;
      this.mapservIP = mapservIP;
      this.tilecacheIP = tilecacheIP;

      var url = jsonLayerCfgFile;
      Ext.Ajax.request({
         url: url,
         scriptTag: true,
         callbackName: "map_config_callback",
         success: this.processConfig,
         failure: function(response, options)
         {
            alert("Unable to retrieve the configuration data to setup the map. Cannot continue.");
            return;
         },
         scope: this
      });
    }
  }
}(); // end of app
