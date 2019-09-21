/**
 * Dependencies
 */
const Schema = require('mongoose').Schema;
const streamWorker = require('stream-worker');
const _isEmpty = require('lodash/isEmpty');

/**
 * Utils
 */
const mpathUtil = {};

mpathUtil.getChildPath = (doc, pathSeparator) => {
  let basePath = _isEmpty(doc.path) ? pathSeparator : doc.path;
  return basePath + doc._id.toString() + pathSeparator;
};
mpathUtil.getLevelByPathAndSeparator = (path, separator) => (path ? path.split(separator).length - 1 : 1);

mpathUtil.listToTree = list => {
  let nodeMap = {};
  let currentNode;
  let rootNodes = [];
  let index;

  for (index = 0; index < list.length; index += 1) {
    currentNode = list[index];
    currentNode.children = [];
    nodeMap[currentNode._id] = index;

    const hasParentInMap = nodeMap.hasOwnProperty(currentNode.parent);

    if (hasParentInMap) {
      list[nodeMap[currentNode.parent]].children.push(currentNode);
    } else {
      rootNodes.push(currentNode);
    }
  }

  return rootNodes;
};

/**
 * Main plugin method
 * @param  {Schema} schema  Mongoose Schema
 * @param  {Object} options [description]
 */
function mpathPlugin(schema, options) {
  const onDelete = (options && options.onDelete) || 'REPARENT'; // or 'DELETE'
  const idType = (options && options.idType) || Schema.ObjectId;
  const pathSeparator = (options && options.pathSeparator) || '#';
  const pathSeparatorRegex = '[' + pathSeparator + ']';
  const emptyPathValue = '';

  const streamWorkerOptions = {
    promises: false,
    concurrency: 5,
  };

  schema.add({
    parent: {
      index: true,
      set: value => (value instanceof Object && value._id ? value._id : value),
      type: idType,
    },
    path: {
      index: true,
      type: String,
      default: emptyPathValue,
    },
  });

  /**
   * Mongoose schema pre save hook
   * @param  {Function} next [description]
   */
  schema.pre('save', function preSave(next) {
    const hasModifiedParent = this.isModified('parent');
    const pathUpdateIsRequired = this.isNew || hasModifiedParent;

    if (!pathUpdateIsRequired) {
      return next();
    }

    const self = this;

    const replacePath = (needle, replacement) => {
      const conditions = { path: { $regex: needle } };
      const childStream = self.collection.find(conditions).stream();
      const onStreamData = (doc, done) => {
        const newChildPath = doc.path.replace(needle, replacement);
        self.collection.updateMany({ _id: doc._id }, { $set: { path: newChildPath } }).then(() => done());
      };
      const onStreamClose = ex => next(ex);
      streamWorker(childStream, onStreamData, streamWorkerOptions, onStreamClose);
    };

    const hasParent = !_isEmpty(this.parent);
    const childUpdateIsRequired = hasModifiedParent && !this.isNew;
    const oldChildPath = mpathUtil.getChildPath(this, pathSeparator);

    if (hasParent) {
      this.collection
        .findOne({ _id: this.parent })
        .then(parentDoc => {
          const newPath = mpathUtil.getChildPath(parentDoc, pathSeparator);
          self.path = newPath;

          if (childUpdateIsRequired) {
            // Rewrite child paths when parent is changed
            const newChildPath = mpathUtil.getChildPath(self, pathSeparator);
            replacePath(oldChildPath, newChildPath);
          } else {
            return next();
          }
        })
        .catch(ex => next(ex));
    } else {
      const newPath = emptyPathValue;
      self.path = newPath;

      if (childUpdateIsRequired) {
        const newChildPath = mpathUtil.getChildPath(self, pathSeparator);
        replacePath(oldChildPath, newChildPath);
      } else {
        return next();
      }
    }
  });

  /**
   * Mongoose schema pre remove hook
   * @param  {Function} next [description]
   */
  schema.pre('remove', function preRemove(next) {
    if (undefined === this.path) {
      return next();
    }

    if ('DELETE' === onDelete) {
      const childPath = mpathUtil.getChildPath(this, pathSeparator);
      const deleteConditions = { path: { $regex: '^' + childPath } };
      this.collection.deleteMany(deleteConditions, next);
    } else {
      // 'REPARENT'
      const parentOfDeletedDoc = this.parent;
      const childConditions = { parent: this._id };
      const childCursor = this.model(this.constructor.modelName)
        .find(childConditions)
        .cursor();

      const onStreamData = (childDoc, done) => {
        childDoc.parent = parentOfDeletedDoc;

        childDoc
          .save()
          .then(() => done())
          .catch(ex => next(ex));
      };

      const onStreamClose = ex => next(ex);

      streamWorker(childCursor, onStreamData, streamWorkerOptions, onStreamClose);
    }
  });

  schema.virtual('level').get(function virtualPropLevel() {
    return mpathUtil.getLevelByPathAndSeparator(this.path, pathSeparator);
  });

  schema.methods.getImmediateChildren = function getImmediateChildren(conditions, fields, options) {
    conditions = conditions || {};
    fields = fields || null;
    options = options || {};

    if (conditions['$query']) {
      conditions['$query']['parent'] = this._id;
    } else {
      conditions['parent'] = this._id;
    }

    return this.model(this.constructor.modelName).find(conditions, fields, options);
  };

  schema.methods.getAllChildren = function getAllChildren(conditions, fields, options) {
    conditions = conditions || {};
    fields = fields || null;
    options = options || {};

    const pathConditions = { $regex: '^' + this.path + pathSeparatorRegex };

    if (conditions['$query']) {
      conditions['$query']['path'] = pathConditions;
    } else {
      conditions['path'] = pathConditions;
    }

    return this.model(this.constructor.modelName).find(conditions, fields, options);
  };

  /**
   * Get parent document
   * @param  {String} fields  [description]
   * @param  {Object} options [description]
   * @return {Prromise.<Mongoose.document>}         [description]
   */
  schema.methods.getParent = function getParent(fields, options) {
    const conditions = { _id: this.parent };

    fields = fields || null;
    options = options || {};

    return this.model(this.constructor.modelName).findOne(conditions, fields, options);
  };

  schema.methods.getAncestors = function getAncestors(conditions = {}, fields = null, options = {}) {
    let ancestorIds = [];

    if (this.path) {
      ancestorIds = this.path.split(pathSeparator);
      ancestorIds.pop();
    }

    if (conditions['$query']) {
      conditions['$query']['_id'] = { $in: ancestorIds };
    } else {
      conditions['_id'] = { $in: ancestorIds };
    }

    return this.model(this.constructor.modelName).find(conditions, fields, options);
  };

  /**
   * Returns tree of child documents
   * @param  {Object} args [description]
   * @return {Promise.<Object>}      [description]
   */
  schema.statics.getChildrenTree = function getChildrenTree(args) {
    const rootDoc = args && args.rootDoc ? args.rootDoc : null;
    let fields = args && args.fields ? args.fields : null;
    let filters = args && args.filters ? args.filters : {};
    let minLevel = args && args.minLevel ? args.minLevel : 1;
    let options = args && args.options ? args.options : {};
    let populateStr = args && args.populate ? args.populate : '';

    // filters
    if (rootDoc) {
      filters.path = { $regex: '^' + mpathUtil.getChildPath(rootDoc, pathSeparator) };
    }

    // fields
    // include 'path' and 'parent' if not already included
    if (fields) {
      if (fields instanceof Object) {
        if (!fields.hasOwnProperty('path')) {
          fields['path'] = 1;
        }
        if (!fields.hasOwnProperty('parent')) {
          fields['parent'] = 1;
        }
      } else {
        if (!fields.match(/path/)) {
          fields += ' path';
        }
        if (!fields.match(/parent/)) {
          fields += ' parent';
        }
      }
    }

    // options:sort
    if (!options.hasOwnProperty('sort')) {
      options.sort = { path: 1 };
    } else if (!options.sort.hasOwnProperty('path')) {
      options.sort.path = 1;
    }

    return this.find(filters, fields, options)
      .populate(populateStr)
      .then(result => result.filter(node => mpathUtil.getLevelByPathAndSeparator(node.path, pathSeparator) >= minLevel))
      .then(result => mpathUtil.listToTree(result))
      .catch(err => console.error(err));
  };

  /**
   * Static method of getChildrenTree schema
   * @param  {Object} args [description]
   * @return {Promise.<Mongoose.document>}      [description]
   */
  schema.methods.getChildrenTree = function(args) {
    args.rootDoc = this;

    return this.constructor.getChildrenTree(args);
  };
}

module.exports = exports = mpathPlugin;
module.exports.util = mpathUtil;
